use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Fields, Ident, Type};

/// Derive macro for Collection trait
///
/// Supports the following attributes:
/// - `#[id]` or `#[key]` - marks the primary key field
/// - `#[index]` - creates a secondary index on the field
/// - `#[index(unique)]` - creates a unique secondary index
///
/// # Generated Code
///
/// - `Collection` trait implementation
/// - `Indexed` trait implementation  
/// - `UserFields` struct with type-safe field accessors
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Collection, Serialize, Deserialize)]
/// struct User {
///     #[key]
///     pub id: String,
///     
///     #[index]
///     pub age: u32,
///     
///     #[index(unique)]
///     pub email: String,
/// }
///
/// // Type-safe field access
/// let field_name = User::fields().age;  // "age" as &'static str
/// ```
#[proc_macro_derive(Collection, attributes(id, key, index, collection))]
pub fn collection_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    // Find ID/Key field
    let (id_field_name, id_field_type) = find_id_field(&input.data)
        .expect("A field with `#[id]` or `#[key]` attribute is required to derive `Collection`.");

    // Find all fields and indexed fields
    let all_fields = find_all_fields(&input.data);
    let indexed_fields = find_indexed_fields(&input.data);

    // Generate field constants struct
    let fields_struct_name = format_ident!("{}Fields", struct_name);
    let field_constants: Vec<_> = all_fields
        .iter()
        .map(|(name, _ty)| {
            quote! {
                pub #name: &'static str
            }
        })
        .collect();
    let field_values: Vec<_> = all_fields
        .iter()
        .map(|(name, _ty)| {
            let name_str = name.to_string();
            quote! {
                #name: #name_str
            }
        })
        .collect();

    // Generate index definitions
    let index_defs: Vec<_> = indexed_fields
        .iter()
        .map(|(name, _ty, unique)| {
            let name_str = name.to_string();
            quote! {
                prkdb_types::index::IndexDef {
                    field: #name_str,
                    unique: #unique,
                }
            }
        })
        .collect();

    // Generate index value extraction
    let index_extractions: Vec<_> = indexed_fields
        .iter()
        .map(|(name, _ty, _unique)| {
            let name_str = name.to_string();
            quote! {
                (#name_str, serde_json::to_vec(&self.#name).unwrap_or_default())
            }
        })
        .collect();

    // Generate QueryBuilder extension methods for indexed fields
    let query_methods: Vec<_> = all_fields
        .iter()
        .map(|(name, ty)| {
            let _name_str = name.to_string();
            let where_eq = format_ident!("where_{}_eq", name);
            let type_str = quote!(#ty).to_string();

            // Generate type-appropriate methods
            // Check for Vec/Array types first (they should use the default handler)
            if type_str.contains("Vec") || type_str.contains("[") {
                // Vec/Array types: just eq with Clone bound
                quote! {
                    /// Filter where #name equals the given value
                    fn #where_eq(self, value: #ty) -> Self
                    where #ty: PartialEq + Clone + 'static
                    {
                        self.filter(move |r| r.#name == value)
                    }
                }
            } else if type_str.contains("String") || type_str.contains("str") {
                // String type: eq, contains, starts_with
                let where_contains = format_ident!("where_{}_contains", name);
                let where_starts_with = format_ident!("where_{}_starts_with", name);
                quote! {
                    /// Filter where #name equals the given value
                    fn #where_eq(self, value: impl AsRef<str> + 'a) -> Self {
                        self.filter(move |r| r.#name.as_str() == value.as_ref())
                    }

                    /// Filter where #name contains the given substring
                    fn #where_contains(self, value: impl AsRef<str> + 'a) -> Self {
                        self.filter(move |r| r.#name.contains(value.as_ref()))
                    }

                    /// Filter where #name starts with the given prefix
                    fn #where_starts_with(self, value: impl AsRef<str> + 'a) -> Self {
                        self.filter(move |r| r.#name.starts_with(value.as_ref()))
                    }
                }
            } else if type_str.contains("u32")
                || type_str.contains("i32")
                || type_str.contains("u64")
                || type_str.contains("i64")
                || type_str.contains("usize")
                || type_str.contains("f32")
                || type_str.contains("f64")
            {
                // Numeric types: eq, gt, lt, gte, lte
                let where_gt = format_ident!("where_{}_gt", name);
                let where_lt = format_ident!("where_{}_lt", name);
                let where_gte = format_ident!("where_{}_gte", name);
                let where_lte = format_ident!("where_{}_lte", name);
                quote! {
                    /// Filter where #name equals the given value
                    fn #where_eq(self, value: #ty) -> Self {
                        self.filter(move |r| r.#name == value)
                    }

                    /// Filter where #name is greater than the given value
                    fn #where_gt(self, value: #ty) -> Self {
                        self.filter(move |r| r.#name > value)
                    }

                    /// Filter where #name is less than the given value
                    fn #where_lt(self, value: #ty) -> Self {
                        self.filter(move |r| r.#name < value)
                    }

                    /// Filter where #name is greater than or equal to the given value
                    fn #where_gte(self, value: #ty) -> Self {
                        self.filter(move |r| r.#name >= value)
                    }

                    /// Filter where #name is less than or equal to the given value
                    fn #where_lte(self, value: #ty) -> Self {
                        self.filter(move |r| r.#name <= value)
                    }
                }
            } else if type_str.contains("bool") {
                // Boolean: is_true, is_false
                let where_true = format_ident!("where_{}_is_true", name);
                let where_false = format_ident!("where_{}_is_false", name);
                quote! {
                    /// Filter where #name equals the given value
                    fn #where_eq(self, value: bool) -> Self {
                        self.filter(move |r| r.#name == value)
                    }

                    /// Filter where #name is true
                    fn #where_true(self) -> Self {
                        self.filter(|r| r.#name)
                    }

                    /// Filter where #name is false
                    fn #where_false(self) -> Self {
                        self.filter(|r| !r.#name)
                    }
                }
            } else {
                // Default: just eq with PartialEq + Clone bound (for enums and custom types)
                quote! {
                    /// Filter where #name equals the given value
                    fn #where_eq(self, value: #ty) -> Self
                    where #ty: PartialEq + Clone + 'static
                    {
                        self.filter(move |r| r.#name == value)
                    }
                }
            }
        })
        .collect();

    // Create QueryBuilder extension trait name
    let query_ext_trait = format_ident!("{}QueryExt", struct_name);

    // Generate the extension trait
    let query_builder_ext = quote! {
        /// Extension trait for type-safe query methods
        pub trait #query_ext_trait<'a, S: prkdb_types::storage::StorageAdapter + 'static>: Sized {
            #(#query_methods)*

            /// Generic filter using a closure
            fn filter<F: Fn(&#struct_name) -> bool + 'a>(self, predicate: F) -> Self;
        }
    };

    let expanded = quote! {
        // Collection trait implementation
        impl prkdb_types::collection::Collection for #struct_name {
            type Id = #id_field_type;
            fn id(&self) -> &Self::Id {
                &self.#id_field_name
            }
        }

        // Indexed trait implementation
        impl prkdb_types::index::Indexed for #struct_name {
            fn indexes() -> &'static [prkdb_types::index::IndexDef] {
                static DEFS: &[prkdb_types::index::IndexDef] = &[
                    #(#index_defs),*
                ];
                DEFS
            }

            fn index_values(&self) -> Vec<(&'static str, Vec<u8>)> {
                vec![
                    #(#index_extractions),*
                ]
            }
        }

        /// Type-safe field name accessors
        #[derive(Debug, Clone, Copy)]
        pub struct #fields_struct_name {
            #(#field_constants),*
        }

        impl #struct_name {
            /// Get type-safe field name accessors
            ///
            /// # Example
            /// ```ignore
            /// let age_field = User::fields().age;  // "age"
            /// db.sum(|u: &User| u.age).await?;     // Use with closures
            /// ```
            pub fn fields() -> #fields_struct_name {
                #fields_struct_name {
                    #(#field_values),*
                }
            }
        }

        #query_builder_ext
    };

    TokenStream::from(expanded)
}

/// Find the ID/Key field (supports both #[id] and #[key])
fn find_id_field(data: &Data) -> Option<(Ident, Type)> {
    if let Data::Struct(s) = data {
        if let Fields::Named(named_fields) = &s.fields {
            for field in &named_fields.named {
                for attr in &field.attrs {
                    if attr.path().is_ident("id") || attr.path().is_ident("key") {
                        return Some((field.ident.clone().unwrap(), field.ty.clone()));
                    }
                }
            }
        }
    }
    None
}

/// Find all fields in the struct
fn find_all_fields(data: &Data) -> Vec<(Ident, Type)> {
    let mut fields = Vec::new();
    if let Data::Struct(s) = data {
        if let Fields::Named(named_fields) = &s.fields {
            for field in &named_fields.named {
                if let Some(ident) = &field.ident {
                    fields.push((ident.clone(), field.ty.clone()));
                }
            }
        }
    }
    fields
}

/// Find all indexed fields
/// Returns: Vec<(field_name, field_type, is_unique)>
fn find_indexed_fields(data: &Data) -> Vec<(Ident, Type, bool)> {
    let mut indexed = Vec::new();

    if let Data::Struct(s) = data {
        if let Fields::Named(named_fields) = &s.fields {
            for field in &named_fields.named {
                for attr in &field.attrs {
                    if attr.path().is_ident("index") {
                        let field_name = field.ident.clone().unwrap();
                        let field_type = field.ty.clone();

                        // Check for #[index(unique)]
                        let is_unique = attr
                            .parse_args::<syn::Ident>()
                            .map(|ident| ident == "unique")
                            .unwrap_or(false);

                        indexed.push((field_name, field_type, is_unique));
                    }
                }
            }
        }
    }

    indexed
}
