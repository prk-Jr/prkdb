//! prkdb-orm-macros - derive(Table)
//!
//! Derive macro that implements:
//!  - `prkdb_orm::schema::Table` for the struct (DDL/DML SQL generation).
//!  - `prkdb_orm::schema::TableHelpers` for the struct (fluent builders).
//!
//! It generates type-safe `where_*`, `or_where_*`, `in_*`, `or_in_*`, `having_*`,
//! `or_having_*`, `group_by_*`, `order_by_*`, join helpers (and compound join+where helpers),
//! `update_<field>_with_value`, and implements traits for external builders.
//!
//! New features in this version:
//!  - `#[auto_increment]` attribute on fields: those fields are excluded from `INSERT` SQL and params.
//!  - `#[validate(...)]` attribute on fields — supports either a function name (e.g. `check_name`)
//!    or a boolean expression (must *use `self`*, e.g. `self.age >= 18`). The macro generates
//!    `pub fn validate_instance(&self) -> Result<(), String>` and calls it from `insert_params`
//!    (panics on validation error because `insert_params` returns `Vec` in the `Table` trait).

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote, ToTokens};
use syn::{
    parse::Parser, punctuated::Punctuated, Attribute, Data, DeriveInput, Expr, Fields, Ident, Lit,
    LitStr, Meta, MetaNameValue, Token, Type,
};

/// Parse `#[table(name = "...", primary_key = "...")]` attribute
fn parse_table_attr(attrs: &[Attribute]) -> Result<(String, Option<String>), syn::Error> {
    for attr in attrs {
        if attr.path().is_ident("table") {
            match &attr.meta {
                Meta::List(list) => {
                    let parser = Punctuated::<MetaNameValue, Token![,]>::parse_terminated;
                    let parsed = parser.parse2(list.tokens.clone()).map_err(|e| {
                        syn::Error::new_spanned(
                            &list.tokens,
                            format!("failed to parse table attribute: {}", e),
                        )
                    })?;

                    let mut table_name: Option<String> = None;
                    let mut primary_key: Option<String> = None;

                    for nv in parsed.into_iter() {
                        if nv.path.is_ident("name") {
                            if let syn::Expr::Lit(expr_lit) = &nv.value {
                                if let Lit::Str(s) = &expr_lit.lit {
                                    table_name = Some(s.value());
                                } else {
                                    return Err(syn::Error::new_spanned(
                                        &expr_lit.lit,
                                        "expected string literal for table name",
                                    ));
                                }
                            } else {
                                return Err(syn::Error::new_spanned(
                                    &nv.value,
                                    "expected string literal for table name",
                                ));
                            }
                        } else if nv.path.is_ident("primary_key") {
                            if let syn::Expr::Lit(expr_lit) = &nv.value {
                                if let Lit::Str(s) = &expr_lit.lit {
                                    primary_key = Some(s.value());
                                } else {
                                    return Err(syn::Error::new_spanned(
                                        &expr_lit.lit,
                                        "expected string literal for primary_key",
                                    ));
                                }
                            } else {
                                return Err(syn::Error::new_spanned(
                                    &nv.value,
                                    "expected string literal for primary_key",
                                ));
                            }
                        } else {
                            return Err(syn::Error::new_spanned(
                                nv.path,
                                "unknown key in table attribute - expected `name` and optional `primary_key`",
                            ));
                        }
                    }

                    if let Some(tn) = table_name {
                        return Ok((tn, primary_key));
                    } else {
                        return Err(syn::Error::new_spanned(
                            attr,
                            "table attribute requires `name = \"...\"`",
                        ));
                    }
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        attr,
                        "expected #[table(name = \"...\")]",
                    ));
                }
            }
        }
    }

    Err(syn::Error::new(
        Span::call_site(),
        "missing required #[table(name = \"...\")] attribute",
    ))
}

/// Field-level column hints.
#[derive(Default, Debug, Clone)]
struct ColumnHints {
    sql_type: Option<String>,
    force_json: bool,
}

/// Parse `#[column(sql_type = "...")]` or `#[column(json)]` on a **field**.
fn parse_column_sql_type(attrs: &[Attribute]) -> Result<ColumnHints, syn::Error> {
    let mut hints = ColumnHints::default();
    for attr in attrs {
        if attr.path().is_ident("column") {
            match &attr.meta {
                Meta::List(list) => {
                    // Accept either `#[column(sql_type = "...")]` or `#[column(json)]` or a list with both separated by commas.
                    let nested = Punctuated::<Meta, Token![,]>::parse_terminated
                        .parse2(list.tokens.clone())
                        .map_err(|e| {
                            syn::Error::new_spanned(
                                &list.tokens,
                                format!("failed to parse column attribute: {}", e),
                            )
                        })?;

                    for meta in nested {
                        match meta {
                            Meta::Path(p) if p.is_ident("json") => {
                                hints.force_json = true;
                            }
                            Meta::NameValue(nv) if nv.path.is_ident("sql_type") => {
                                if let syn::Expr::Lit(expr_lit) = &nv.value {
                                    if let Lit::Str(s) = &expr_lit.lit {
                                        hints.sql_type = Some(s.value());
                                    } else {
                                        return Err(syn::Error::new_spanned(
                                            &expr_lit.lit,
                                            "expected string literal for sql_type",
                                        ));
                                    }
                                } else {
                                    return Err(syn::Error::new_spanned(
                                        &nv.value,
                                        "expected string literal for sql_type",
                                    ));
                                }
                            }
                            other => {
                                return Err(syn::Error::new_spanned(
                                    other,
                                    "expected `json` or `sql_type = \"...\"`",
                                ))
                            }
                        }
                    }
                }
                _ => {
                    if let Meta::Path(_) = &attr.meta {
                        hints.force_json = true;
                    } else {
                        return Err(syn::Error::new_spanned(
                            attr,
                            "expected #[column(sql_type = \"...\")] or #[column(json)]",
                        ));
                    }
                }
            }
        }
    }

    Ok(hints)
}

/// Converts a `Type` to the last (unqualified) identifier string, if possible:
/// e.g. `String` -> "String", `chrono::NaiveDateTime` -> "NaiveDateTime"
fn simple_type_name(ty: &Type) -> Option<String> {
    if let Type::Path(tp) = ty {
        if tp.qself.is_none() && !tp.path.segments.is_empty() {
            let last = tp.path.segments.last().unwrap();
            return Some(last.ident.to_string());
        }
    }
    None
}

/// Field-level validator representation we collect while parsing.
enum FieldValidator {
    /// Function name (path)
    Function(syn::Path),
    /// Boolean expression (must reference `self`, e.g. `self.age >= 18`)
    Expr(syn::Expr),
}

/// Return `Some(inner_type)` if `ty` is `Option<inner_type>`.
fn option_inner_type(ty: &Type) -> Option<Type> {
    if let Type::Path(tp) = ty {
        if tp.qself.is_none()
            && tp.path.segments.len() == 1
            && tp.path.segments[0].ident == "Option"
        {
            if let syn::PathArguments::AngleBracketed(args) = &tp.path.segments[0].arguments {
                if args.args.len() == 1 {
                    if let syn::GenericArgument::Type(inner) = &args.args[0] {
                        return Some(inner.clone());
                    }
                }
            }
        }
    }
    None
}

/// Return Some(inner_type) if ty is Vec<inner_type>
fn vec_inner_type(ty: &Type) -> Option<Type> {
    if let Type::Path(tp) = ty {
        if tp.qself.is_none() && tp.path.segments.len() == 1 && tp.path.segments[0].ident == "Vec" {
            if let syn::PathArguments::AngleBracketed(args) = &tp.path.segments[0].arguments {
                if args.args.len() == 1 {
                    if let syn::GenericArgument::Type(inner) = &args.args[0] {
                        return Some(inner.clone());
                    }
                }
            }
        }
    }
    None
}

#[derive(Default)]
struct JoinInfo {
    target: Option<String>,
    on: Option<String>,
}

fn parse_join_attr(attr: &Attribute) -> Result<JoinInfo, syn::Error> {
    let mut info = JoinInfo::default();
    match &attr.meta {
        Meta::List(list) => {
            let parsed = Punctuated::<Meta, Token![,]>::parse_terminated
                .parse2(list.tokens.clone())
                .map_err(|e| {
                    syn::Error::new_spanned(
                        &list.tokens,
                        format!("failed to parse join attribute: {e}"),
                    )
                })?;
            for meta in parsed {
                match meta {
                    Meta::Path(p) => {
                        info.target = Some(
                            p.get_ident()
                                .map(|i| i.to_string())
                                .unwrap_or_else(|| p.to_token_stream().to_string()),
                        );
                    }
                    Meta::NameValue(nv) if nv.path.is_ident("on") => {
                        if let syn::Expr::Lit(expr_lit) = &nv.value {
                            if let Lit::Str(s) = &expr_lit.lit {
                                info.on = Some(s.value());
                            } else {
                                return Err(syn::Error::new_spanned(
                                    &expr_lit.lit,
                                    "expected string literal for on",
                                ));
                            }
                        } else {
                            return Err(syn::Error::new_spanned(
                                &nv.value,
                                "expected string literal for on",
                            ));
                        }
                    }
                    other => {
                        return Err(syn::Error::new_spanned(
                            other,
                            "expected join target type and optional on = \"...\"",
                        ));
                    }
                }
            }
        }
        _ => {
            return Err(syn::Error::new_spanned(
                attr,
                "expected #[join(TargetTable, on = \"lhs = rhs\")]",
            ));
        }
    }
    Ok(info)
}

fn parse_foreign_attr(attr: &Attribute) -> Result<String, syn::Error> {
    match &attr.meta {
        Meta::List(list) => {
            let tokens_string = list.tokens.to_string().replace(' ', "");
            Ok(tokens_string)
        }
        _ => Err(syn::Error::new_spanned(
            attr,
            "expected #[foreign(Target::field)]",
        )),
    }
}

#[proc_macro_derive(
    Table,
    attributes(table, column, auto_increment, validate, foreign, join)
)]
pub fn derive_table(input: TokenStream) -> TokenStream {
    // parse
    let input = match syn::parse::<DeriveInput>(input) {
        Ok(i) => i,
        Err(e) => return e.to_compile_error().into(),
    };

    let struct_ident = input.ident.clone();
    let (table_name, mut pk_field_name_opt) = match parse_table_attr(&input.attrs) {
        Ok(x) => x,
        Err(e) => return e.to_compile_error().into(),
    };

    // named struct
    let named_fields = match input.data {
        Data::Struct(ds) => match ds.fields {
            Fields::Named(named) => named.named,
            _ => {
                return syn::Error::new_spanned(
                    struct_ident,
                    "derive(Table) supports structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                struct_ident,
                "derive(Table) can only be derived for structs",
            )
            .to_compile_error()
            .into();
        }
    };

    // If pk_field_name is None, try to find a field with #[id]
    if pk_field_name_opt.is_none() {
        for field in named_fields.iter() {
            for attr in &field.attrs {
                if attr.path().is_ident("id") {
                    if let Some(ident) = &field.ident {
                        pk_field_name_opt = Some(ident.to_string());
                        break;
                    }
                }
            }
            if pk_field_name_opt.is_some() {
                break;
            }
        }
    }

    let pk_field_name = match pk_field_name_opt {
        Some(pk) => pk,
        None => {
            return syn::Error::new_spanned(
                struct_ident,
                "derive(Table) requires `primary_key` in #[table(...)] or `#[id]` on a field",
            )
            .to_compile_error()
            .into();
        }
    };

    // cache the pk literal for inference logic
    let pk_field_name_lit = LitStr::new(&pk_field_name, Span::call_site());

    // collect idents, types, literal names and optional field sql_type override
    let mut col_idents: Vec<Ident> = Vec::new();
    let mut col_types: Vec<Type> = Vec::new();
    let mut col_name_lits: Vec<LitStr> = Vec::new();
    let mut col_sql_type_overrides: Vec<Option<LitStr>> = Vec::new();
    let mut col_force_json: Vec<bool> = Vec::new();
    let mut col_auto_increment: Vec<bool> = Vec::new();
    let mut col_validators: Vec<Vec<FieldValidator>> = Vec::new();
    let mut col_foreign: Vec<Option<String>> = Vec::new();
    let mut col_joins: Vec<Option<(String, Option<String>)>> = Vec::new();

    // join fields are not stored; they are defaulted in FromRow
    let mut join_defaults: Vec<TokenStream2> = Vec::new();
    // Join metadata is gathered as token streams so we can build SQL-friendly values lazily.
    let mut join_metadata: Vec<TokenStream2> = Vec::new();

    // Local enum to make joins readable
    enum JoinKind {
        Explicit { target: String, on: Option<String> },
        Inferred { target_ty: Type },
    }

    for field in named_fields.iter() {
        if let Some(ident) = &field.ident {
            let has_column_attr = field
                .attrs
                .iter()
                .any(|attr| attr.path().is_ident("column"));

            // detect join (attribute or Vec<T> inference)
            let mut join: Option<JoinKind> = None;
            for attr in &field.attrs {
                if attr.path().is_ident("join") {
                    match parse_join_attr(attr) {
                        Ok(info) => {
                            if let Some(target) = info.target {
                                join = Some(JoinKind::Explicit {
                                    target,
                                    on: info.on,
                                });
                            }
                        }
                        Err(e) => return e.to_compile_error().into(),
                    }
                }
            }
            if join.is_none() {
                if let Some(inner_ty) = vec_inner_type(&field.ty) {
                    // Without an explicit column hint, treat Vec<T> as a virtual join field.
                    if !has_column_attr {
                        join = Some(JoinKind::Inferred {
                            target_ty: inner_ty,
                        });
                    }
                }
            }

            if let Some(join_kind) = join {
                // Skip persistence columns, set default init in FromRow.
                join_defaults.push(quote! { #ident: Default::default() });

                // Record join metadata
                let field_lit = LitStr::new(&ident.to_string(), Span::call_site());
                match join_kind {
                    JoinKind::Explicit { target, on } => {
                        let table_token: TokenStream2 = syn::parse_str::<Type>(&target)
                            .map(|ty| quote! { <#ty as ::prkdb_orm::schema::Table>::table_name() })
                            .unwrap_or_else(|_| {
                                let lit = LitStr::new(&target, Span::call_site());
                                quote! { #lit }
                            });
                        let on_lit = LitStr::new(&on.unwrap_or_default(), Span::call_site());
                        join_metadata.push(quote! {{
                            let table: &'static str = #table_token;
                            (#field_lit, table, #on_lit)
                        }});
                    }
                    JoinKind::Inferred { target_ty } => {
                        join_metadata.push(quote! {{
                            let fk = <#target_ty as ::prkdb_orm::schema::Relations>::foreign_keys()
                                .iter()
                                .find(|(_, rhs)| *rhs == concat!(stringify!(#struct_ident), "::", #pk_field_name_lit))
                                .map(|(lhs, _)| *lhs)
                                .unwrap_or("id");
                            let on = Box::leak(format!(
                                "{}.{} = {}.{}",
                                <#struct_ident as ::prkdb_orm::schema::Table>::table_name(),
                                <#struct_ident as ::prkdb_orm::schema::Table>::pk_column(),
                                <#target_ty as ::prkdb_orm::schema::Table>::table_name(),
                                fk
                            ).into_boxed_str());
                            (#field_lit, <#target_ty as ::prkdb_orm::schema::Table>::table_name(), on)
                        }});
                    }
                }
                // do not push into column vectors
                continue;
            }

            col_idents.push(ident.clone());
            col_types.push(field.ty.clone());
            col_name_lits.push(LitStr::new(&ident.to_string(), Span::call_site()));

            // column(sql_type = "...") optional override
            match parse_column_sql_type(&field.attrs) {
                Ok(hints) => {
                    if let Some(s) = hints.sql_type {
                        col_sql_type_overrides.push(Some(LitStr::new(&s, Span::call_site())))
                    } else {
                        col_sql_type_overrides.push(None);
                    }
                    col_force_json.push(hints.force_json);
                }
                Err(e) => return e.to_compile_error().into(),
            };

            // auto_increment attribute
            let mut is_auto_inc = false;
            for attr in &field.attrs {
                if attr.path().is_ident("auto_increment") {
                    is_auto_inc = true;
                    break;
                }
            }
            col_auto_increment.push(is_auto_inc);

            // validate attribute(s) — allow multiple validate attributes on same field
            let mut validators_for_field: Vec<FieldValidator> = Vec::new();
            for attr in &field.attrs {
                if attr.path().is_ident("validate") {
                    // try parse as Expr; parse_args will parse the tokens inside parentheses
                    match attr.parse_args::<Expr>() {
                        Ok(expr) => {
                            // if it's a path-like expression (bare identifier), treat as function
                            match &expr {
                                Expr::Path(p)
                                    if p.qself.is_none() && p.path.segments.len() == 1 =>
                                {
                                    validators_for_field
                                        .push(FieldValidator::Function(p.path.clone()));
                                }
                                // otherwise treat as boolean expression; require user to reference `self`
                                other_expr => {
                                    validators_for_field
                                        .push(FieldValidator::Expr(other_expr.clone()));
                                }
                            }
                        }
                        Err(e) => {
                            return syn::Error::new_spanned(
                                attr,
                                format!("failed to parse validate attribute: {}", e),
                            )
                            .to_compile_error()
                            .into();
                        }
                    }
                }
            }
            col_validators.push(validators_for_field);

            // foreign attribute
            let mut fk: Option<String> = None;
            for attr in &field.attrs {
                if attr.path().is_ident("foreign") {
                    match parse_foreign_attr(attr) {
                        Ok(val) => fk = Some(val),
                        Err(e) => return e.to_compile_error().into(),
                    }
                }
            }
            col_foreign.push(fk);

            col_joins.push(None);
        }
    }

    // pk type
    let pk_ident_for_type = Ident::new(&pk_field_name, Span::call_site());
    let mut pk_type_opt: Option<Type> = None;
    for (i, ident) in col_idents.iter().enumerate() {
        if ident == &pk_ident_for_type {
            pk_type_opt = Some(col_types[i].clone());
            break;
        }
    }
    let pk_type = match pk_type_opt {
        Some(t) => t,
        None => {
            return syn::Error::new_spanned(
                struct_ident,
                format!(
                    "primary_key '{}' not found among struct fields",
                    pk_field_name
                ),
            )
            .to_compile_error()
            .into();
        }
    };

    // column meta tokens (use override if present, otherwise call OrmType::sql_type_name(dialect))
    let cols_meta_tokens: Vec<_> = col_idents
        .iter()
        .zip(col_types.iter())
        .zip(col_sql_type_overrides.iter())
        .zip(col_force_json.iter())
        .zip(col_auto_increment.iter())
        .map(|((((ident, ty), override_opt), force_json), auto_inc)| {
            if let Some(lit) = override_opt {
                quote! { ( stringify!(#ident), #lit ) }
            } else if *auto_inc {
                // Dialect-aware auto increment type hints
                quote! {
                    ( stringify!(#ident),
                      match dialect {
                        ::prkdb_orm::dialect::SqlDialect::Sqlite => "INTEGER PRIMARY KEY AUTOINCREMENT",
                        ::prkdb_orm::dialect::SqlDialect::Postgres => "BIGSERIAL",
                        ::prkdb_orm::dialect::SqlDialect::MySql => "BIGINT AUTO_INCREMENT",
                      }
                    )
                }
            } else if *force_json {
                quote! { ( stringify!(#ident), <::serde_json::Value as ::prkdb_orm::types::OrmType>::sql_type_name(dialect) ) }
            } else {
                quote! { ( stringify!(#ident), <#ty as ::prkdb_orm::types::OrmType>::sql_type_name(dialect) ) }
            }
        })
        .collect();

    // Build insert-related tokens while skipping auto-increment fields
    let mut insert_col_name_lits: Vec<LitStr> = Vec::new();
    let mut insert_push_tokens: Vec<TokenStream2> = Vec::new();
    let mut from_row_field_inits: Vec<TokenStream2> = Vec::new();
    for (i, ident) in col_idents.iter().enumerate() {
        let field_ty = &col_types[i];
        let option_inner = option_inner_type(field_ty);
        let is_auto_inc = col_auto_increment.get(i).copied().unwrap_or(false);

        if !is_auto_inc {
            insert_col_name_lits.push(LitStr::new(&ident.to_string(), Span::call_site()));
            let push_ts = if col_force_json.get(i).copied().unwrap_or(false) {
                if option_inner.is_some() {
                    quote! {
                        match &self.#ident {
                            None => v.push(::prkdb_orm::types::SqlValue::Null),
                            Some(inner) => v.push(::prkdb_orm::types::SqlValue::Json(
                                ::serde_json::to_string(inner)
                                    .expect("failed to serialize field tagged with #[column(json)]")
                            )),
                        }
                    }
                } else {
                    quote! {
                        v.push(::prkdb_orm::types::SqlValue::Json(
                            ::serde_json::to_string(&self.#ident)
                                .expect("failed to serialize field tagged with #[column(json)]")
                        ));
                    }
                }
            } else if option_inner.is_some() {
                quote! {
                    match &self.#ident {
                        None => v.push(::prkdb_orm::types::SqlValue::Null),
                        Some(inner) => v.push(::prkdb_orm::types::SqlValue::from(inner)),
                    }
                }
            } else {
                quote! {
                    v.push(::prkdb_orm::types::SqlValue::from(&self.#ident));
                }
            };
            insert_push_tokens.push(push_ts);
        }

        // decode for FromRow
        let col_lit = LitStr::new(&ident.to_string(), Span::call_site());
        let force_json = col_force_json.get(i).copied().unwrap_or(false);
        let is_option = option_inner.clone();

        let decode_expr = if force_json {
            if let Some(inner_ty) = is_option {
                quote! {
                    {
                        match row.get(#col_lit) {
                            None | Some(::prkdb_orm::types::SqlValue::Null) => None,
                            Some(val) => {
                                match val {
                                    ::prkdb_orm::types::SqlValue::Json(s) |
                                    ::prkdb_orm::types::SqlValue::Text(s) => {
                                        Some(::serde_json::from_str::<#inner_ty>(s)
                                            .map_err(|e| format!("failed to decode JSON for column {}: {}", #col_lit, e))?)
                                    }
                                    other => {
                                        return Err(format!("expected JSON for column {}, got {:?}", #col_lit, other));
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                quote! {
                    {
                        let raw = row.get(#col_lit).ok_or_else(|| format!("missing column {}", #col_lit))?;
                        match raw {
                            ::prkdb_orm::types::SqlValue::Json(s) |
                            ::prkdb_orm::types::SqlValue::Text(s) => {
                                ::serde_json::from_str::<#field_ty>(s)
                                    .map_err(|e| format!("failed to decode JSON for column {}: {}", #col_lit, e))?
                            }
                            ::prkdb_orm::types::SqlValue::Null => {
                                return Err(format!("null value for non-null column {}", #col_lit));
                            }
                            other => return Err(format!("expected JSON for column {}, got {:?}", #col_lit, other)),
                        }
                    }
                }
            }
        } else if let Some(inner_ty) = is_option {
            quote! {
                {
                    match row.get(#col_lit) {
                        None | Some(::prkdb_orm::types::SqlValue::Null) => None,
                        Some(val) => Some(<#inner_ty as ::prkdb_orm::types::FromSqlValue>::from_sql_value(val)
                            .map_err(|e| format!("failed to decode column {}: {}", #col_lit, e))?),
                    }
                }
            }
        } else {
            quote! {
                {
                    let raw = row.get(#col_lit).ok_or_else(|| format!("missing column {}", #col_lit))?;
                    <#field_ty as ::prkdb_orm::types::FromSqlValue>::from_sql_value(raw)
                        .map_err(|e| format!("failed to decode column {}: {}", #col_lit, e))?
                }
            }
        };

        from_row_field_inits.push(quote! { #ident: #decode_expr });
    }

    let foreign_metadata_tokens: Vec<TokenStream2> = col_idents
        .iter()
        .zip(col_foreign.iter())
        .filter_map(|(ident, fk)| {
            fk.as_ref().map(|target| {
                let f = LitStr::new(&ident.to_string(), Span::call_site());
                let t = LitStr::new(target, Span::call_site());
                quote! { (#f, #t) }
            })
        })
        .collect();

    let join_metadata_tokens: Vec<TokenStream2> = join_metadata;

    let table_name_lit = LitStr::new(&table_name, Span::call_site());

    // Prepare containers for trait signatures and impl bodies (ensures 1-to-1)
    let mut select_sigs: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut select_impls: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut insert_sigs: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut insert_impls: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut update_sigs: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut update_impls: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut delete_sigs: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut delete_impls: Vec<proc_macro2::TokenStream> = Vec::new();

    // iterate fields and generate unique method names/signatures/bodies once
    for (idx, (ident, ty)) in col_idents.iter().zip(col_types.iter()).enumerate() {
        let name_str = ident.to_string();
        let where_eq_fn = format_ident!("where_{}_eq", ident);
        let or_where_fn = format_ident!("or_where_{}_eq", ident);
        let in_fn = format_ident!("in_{}", ident);
        let or_in_fn = format_ident!("or_in_{}", ident);
        let update_set_fn = format_ident!("update_{}_with_value", ident);
        let group_by_fn = format_ident!("group_by_{}", ident);
        let order_by_fn = format_ident!("order_by_{}", ident);
        let having_eq_fn = format_ident!("having_{}_eq", ident);
        let or_having_eq_fn = format_ident!("or_having_{}_eq", ident);

        // join compound names
        let left_join_where_fn = format_ident!("left_join_where_{}_eq", ident);
        let or_left_join_where_fn = format_ident!("or_left_join_where_{}_eq", ident);
        let join_where_fn = format_ident!("join_where_{}_eq", ident);
        let or_join_where_fn = format_ident!("or_join_where_{}_eq", ident);
        let right_join_where_fn = format_ident!("right_join_where_{}_eq", ident);
        let or_right_join_where_fn = format_ident!("or_right_join_where_{}_eq", ident);

        // Add signatures to select trait
        select_sigs.push(
            quote! { fn #where_eq_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; },
        );
        select_sigs.push(
            quote! { fn #or_where_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; },
        );
        select_sigs
            .push(quote! { fn #in_fn(self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self; });
        select_sigs
            .push(quote! { fn #or_in_fn(self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self; });
        select_sigs.push(quote! { fn #group_by_fn(self) -> Self; });
        select_sigs.push(quote! { fn #order_by_fn(self, asc: bool) -> Self; });
        select_sigs.push(
            quote! { fn #having_eq_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; },
        );
        select_sigs.push(quote! { fn #or_having_eq_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });

        // join compound signatures
        select_sigs.push(quote! { fn #left_join_where_fn(self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
        select_sigs.push(quote! { fn #or_left_join_where_fn(self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
        select_sigs.push(quote! { fn #join_where_fn(self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
        select_sigs.push(quote! { fn #or_join_where_fn(self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
        select_sigs.push(quote! { fn #right_join_where_fn(self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
        select_sigs.push(quote! { fn #or_right_join_where_fn(self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });

        // insert signatures (skip auto-increment fields)
        if !col_auto_increment.get(idx).copied().unwrap_or(false) {
            let insert_set_fn = format_ident!("insert_{}", ident);
            insert_sigs.push(
                quote! { fn #insert_set_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; },
            );
            insert_impls.push(quote! {
                fn #insert_set_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                    self = self.add(stringify!(#ident), v.into());
                    self
                }
            });
        }

        // update signatures
        update_sigs.push(
            quote! { fn #update_set_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; },
        );
        update_sigs.push(
            quote! { fn #where_eq_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; },
        );
        update_sigs.push(
            quote! { fn #or_where_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; },
        );
        update_sigs
            .push(quote! { fn #in_fn(self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self; });
        update_sigs
            .push(quote! { fn #or_in_fn(self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self; });

        // delete signatures
        delete_sigs.push(
            quote! { fn #where_eq_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; },
        );
        delete_sigs.push(
            quote! { fn #or_where_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; },
        );
        delete_sigs
            .push(quote! { fn #in_fn(self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self; });
        delete_sigs
            .push(quote! { fn #or_in_fn(self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self; });

        // Now add impl bodies (select)
        select_impls.push(quote! {
            fn #where_eq_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_where_eq(#name_str, v.into());
                self
            }
            fn #or_where_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_or_where_eq(#name_str, v.into());
                self
            }
            fn #in_fn(mut self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_where_in(#name_str, vals);
                self
            }
            fn #or_in_fn(mut self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_or_where_in(#name_str, vals);
                self
            }
            fn #group_by_fn(mut self) -> Self {
                self = self.add_group_by(#name_str);
                self
            }
            fn #order_by_fn(mut self, asc: bool) -> Self {
                self = self.add_order_by(#name_str, asc);
                self
            }
            fn #having_eq_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_having_eq(#name_str, v.into());
                self
            }
            fn #or_having_eq_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_or_having_eq(#name_str, v.into());
                self
            }

            fn #left_join_where_fn(mut self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_join("LEFT JOIN", other, on);
                self = self.add_where_eq(#name_str, v.into());
                self
            }
            fn #or_left_join_where_fn(mut self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_join("LEFT JOIN", other, on);
                self = self.add_or_where_eq(#name_str, v.into());
                self
            }
            fn #join_where_fn(mut self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_join("INNER JOIN", other, on);
                self = self.add_where_eq(#name_str, v.into());
                self
            }
            fn #or_join_where_fn(mut self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_join("INNER JOIN", other, on);
                self = self.add_or_where_eq(#name_str, v.into());
                self
            }
            fn #right_join_where_fn(mut self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_join("RIGHT JOIN", other, on);
                self = self.add_where_eq(#name_str, v.into());
                self
            }
            fn #or_right_join_where_fn(mut self, other: &str, on: &str, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_join("RIGHT JOIN", other, on);
                self = self.add_or_where_eq(#name_str, v.into());
                self
            }
        });

        // update impls
        update_impls.push(quote! {
            fn #update_set_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_update_value(#name_str, v.into());
                self
            }
            fn #where_eq_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_where_eq(#name_str, v.into());
                self
            }
            fn #or_where_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_or_where_eq(#name_str, v.into());
                self
            }
            fn #in_fn(mut self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_where_in(#name_str, vals);
                self
            }
            fn #or_in_fn(mut self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_or_where_in(#name_str, vals);
                self
            }
        });

        // delete impls
        delete_impls.push(quote! {
            fn #where_eq_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_where_eq(#name_str, v.into());
                self
            }
            fn #or_where_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_or_where_eq(#name_str, v.into());
                self
            }
            fn #in_fn(mut self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_where_in(#name_str, vals);
                self
            }
            fn #or_in_fn(mut self, vals: Vec<::prkdb_orm::types::SqlValue>) -> Self {
                self = self.add_or_where_in(#name_str, vals);
                self
            }
        });

        // Type-aware extras
        if let Some(simple) = simple_type_name(ty) {
            match simple.as_str() {
                "String" | "str" | "&str" => {
                    let where_like_ident = format_ident!("where_{}_like", ident);
                    let where_contains_ident = format_ident!("where_{}_contains", ident);
                    let where_starts_with_ident = format_ident!("where_{}_starts_with", ident);
                    let where_ends_with_ident = format_ident!("where_{}_ends_with", ident);

                    // add signatures to trait (these are additional methods)
                    select_sigs.push(quote! { fn #where_like_ident(self, pattern: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
                    select_sigs.push(quote! { fn #where_contains_ident(self, text: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
                    select_sigs.push(quote! { fn #where_starts_with_ident(self, text: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
                    select_sigs.push(quote! { fn #where_ends_with_ident(self, text: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });

                    // and implementations into impl block
                    select_impls.push(quote! {
                        fn #where_like_ident(mut self, pattern: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                            self = self.add_where_like(#name_str, pattern.into());
                            self
                        }
                        fn #where_contains_ident(mut self, text: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                            let v = text.into();
                            self = self.add_where_like(#name_str, v);
                            self
                        }
                        fn #where_starts_with_ident(mut self, text: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                            let v = text.into();
                            self = self.add_where_like(#name_str, v);
                            self
                        }
                        fn #where_ends_with_ident(mut self, text: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                            let v = text.into();
                            self = self.add_where_like(#name_str, v);
                            self
                        }
                    });
                }
                "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" | "f32" | "f64" => {
                    let gt_fn = format_ident!("where_{}_gt", ident);
                    let gte_fn = format_ident!("where_{}_gte", ident);
                    let lt_fn = format_ident!("where_{}_lt", ident);
                    let lte_fn = format_ident!("where_{}_lte", ident);
                    let between_fn = format_ident!("where_{}_between", ident);

                    select_sigs.push(quote! { fn #gt_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
                    select_sigs.push(quote! { fn #gte_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
                    select_sigs.push(quote! { fn #lt_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
                    select_sigs.push(quote! { fn #lte_fn(self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });
                    select_sigs.push(quote! { fn #between_fn(self, lo: impl Into<::prkdb_orm::types::SqlValue>, hi: impl Into<::prkdb_orm::types::SqlValue>) -> Self; });

                    select_impls.push(quote! {
                        fn #gt_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                            self = self.add_where_custom(#name_str, ">", v.into());
                            self
                        }
                        fn #gte_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                            self = self.add_where_custom(#name_str, ">=", v.into());
                            self
                        }
                        fn #lt_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                            self = self.add_where_custom(#name_str, "<", v.into());
                            self
                        }
                        fn #lte_fn(mut self, v: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                            self = self.add_where_custom(#name_str, "<=", v.into());
                            self
                        }
                        fn #between_fn(mut self, lo: impl Into<::prkdb_orm::types::SqlValue>, hi: impl Into<::prkdb_orm::types::SqlValue>) -> Self {
                            self = self.add_where_custom(#name_str, ">=", lo.into());
                            self = self.add_where_custom(#name_str, "<=", hi.into());
                            self
                        }
                    });
                }
                _ => {
                    // no extra helpers for other types
                }
            }
        }
    } // end fields loop

    // Add general/select helper signatures once
    select_sigs.push(quote! {
        fn join(self, other: &str, on: &str) -> Self;
        fn left_join(self, other: &str, on: &str) -> Self;
        fn right_join(self, other: &str, on: &str) -> Self;
        fn order_by(self, col: &str, asc: bool) -> Self;
        fn limit(self, n: usize) -> Self;
        fn offset(self, n: usize) -> Self;
        fn to_sql(self) -> (String, Vec<::prkdb_orm::types::SqlValue>);
    });

    update_sigs.push(quote! {
        fn to_sql(self) -> (String, Vec<::prkdb_orm::types::SqlValue>);
    });

    delete_sigs.push(quote! {
        fn limit(self, n: usize) -> Self;
        fn offset(self, n: usize) -> Self;
        fn to_sql(self) -> (String, Vec<::prkdb_orm::types::SqlValue>);
    });

    // Trait idents
    let select_trait_ident = format_ident!("{}SelectExt", struct_ident);
    let insert_trait_ident = format_ident!("{}InsertExt", struct_ident);
    let update_trait_ident = format_ident!("{}UpdateExt", struct_ident);
    let delete_trait_ident = format_ident!("{}DeleteExt", struct_ident);
    let _from_row_trait_ident = format_ident!("{}FromRowExt", struct_ident);

    // Generate validation method body by iterating over col_validators
    let mut validate_checks: Vec<TokenStream2> = Vec::new();
    for (i, validators) in col_validators.iter().enumerate() {
        let field_ident = &col_idents[i];
        for v in validators {
            match v {
                FieldValidator::Function(path) => {
                    // call function with &self.field_ident
                    let msg_field = field_ident.to_string();
                    validate_checks.push(quote! {
                        match #path(&self.#field_ident) {
                            Ok(_) => {},
                            Err(err) => {
                                return Err(format!("validation failed for field '{}': {:?}", #msg_field, err));
                            }
                        }
                    });
                }
                FieldValidator::Expr(expr) => {
                    // use the expression directly — user must write expressions referencing `self`
                    let msg_field = field_ident.to_string();
                    validate_checks.push(quote! {
                        if !(#expr) {
                            return Err(format!("validation failed for field '{}'", #msg_field));
                        }
                    });
                }
            }
        }
    }

    // For insert SQL/params we need columns list and placeholders for non-auto-inc columns
    let insert_cols_count = insert_col_name_lits.len();
    let _placeholders_for_insert = quote! {
        {
            let placeholders: Vec<String> = (1..=#insert_cols_count).map(|i| dialect.placeholder_at(i)).collect();
            placeholders.join(", ")
        }
    };

    // Build insert column list string for insert_sql (non-auto-inc columns only)
    // We'll create a runtime join using the insert_col_name_lits so dialect.quote_ident used at runtime.
    let _insert_col_list_runtime = {
        let names = insert_col_name_lits.iter();
        quote! {
            {
                let names: &[&str] = &[
                    #(#names),*
                ];
                names.join(", ")
            }
        }
    };

    // Build insert_sql implementation depending on non-auto-inc columns
    let expanded = quote! {
        // Implement Table trait with DDL/DML
        impl ::prkdb_orm::schema::Table for #struct_ident {
            type Id = #pk_type;

            fn table_name() -> &'static str {
                #table_name_lit
            }

            fn columns() -> &'static [&'static str] {
                static COLUMNS: &[&str] = &[
                    #(#col_name_lits),*
                ];
                COLUMNS
            }

            fn columns_meta(dialect: ::prkdb_orm::dialect::SqlDialect) -> Vec<(&'static str, &'static str)> {
                vec![
                    #(#cols_meta_tokens),*
                ]
            }

            fn pk_column() -> &'static str {
                #pk_field_name_lit
            }

            fn create_table_sql(dialect: ::prkdb_orm::dialect::SqlDialect) -> String {
                let cols = <#struct_ident as ::prkdb_orm::schema::Table>::columns_meta(dialect);
                let mut defs: Vec<String> = Vec::new();
                for (col, typ) in cols {
                    let mut def = format!("{} {}", dialect.quote_ident(col), typ);
                    if col == <#struct_ident as ::prkdb_orm::schema::Table>::pk_column() {
                        let lower = typ.to_ascii_lowercase();
                        if !lower.contains("primary key") {
                            def.push_str(" PRIMARY KEY");
                        }
                    }
                    defs.push(def);
                }
                format!("CREATE TABLE IF NOT EXISTS {} (\n    {}\n);", dialect.quote_ident(#table_name_lit), defs.join(",\n    "))
            }

            fn drop_table_sql(dialect: ::prkdb_orm::dialect::SqlDialect) -> String {
                format!("DROP TABLE IF EXISTS {}", dialect.quote_ident(#table_name_lit))
            }

            fn select_all_sql(dialect: ::prkdb_orm::dialect::SqlDialect) -> String {
                let cols = <#struct_ident as ::prkdb_orm::schema::Table>::columns().join(", ");
                format!("SELECT {} FROM {}", cols, dialect.quote_ident(#table_name_lit))
            }

            fn select_by_pk_sql(dialect: ::prkdb_orm::dialect::SqlDialect) -> String {
                let cols = <#struct_ident as ::prkdb_orm::schema::Table>::columns().join(", ");
                let pk_ph = dialect.placeholder_at(1);
                format!("SELECT {} FROM {} WHERE {} = {}", cols, dialect.quote_ident(#table_name_lit), dialect.quote_ident(#pk_field_name_lit), pk_ph)
            }

            fn insert_sql(dialect: ::prkdb_orm::dialect::SqlDialect) -> String {
                // Non-auto-increment columns only
                let col_list = {
                    let names: &[&str] = &[
                        #(#insert_col_name_lits),*
                    ];
                    names.join(", ")
                };
                let placeholders: Vec<String> = (1..=col_list.split(",").count()).map(|i| dialect.placeholder_at(i)).collect();
                let placeholders_joined = placeholders.join(", ");
                format!("INSERT INTO {} ({}) VALUES ({})", dialect.quote_ident(#table_name_lit), col_list, placeholders_joined)
            }

            fn update_sql(dialect: ::prkdb_orm::dialect::SqlDialect) -> String {
                let cols_vec: Vec<&str> = <#struct_ident as ::prkdb_orm::schema::Table>::columns().iter().copied().filter(|c| *c != #pk_field_name_lit).collect();
                let set_parts: Vec<String> = cols_vec.iter().enumerate().map(|(i, c)| {
                    let ph = dialect.placeholder_at(i+1);
                    format!("{} = {}", dialect.quote_ident(c), ph)
                }).collect();
                let pk_ph = dialect.placeholder_at(cols_vec.len()+1);
                format!("UPDATE {} SET {} WHERE {} = {}", dialect.quote_ident(#table_name_lit), set_parts.join(", "), dialect.quote_ident(#pk_field_name_lit), pk_ph)
            }

            fn delete_sql(dialect: ::prkdb_orm::dialect::SqlDialect) -> String {
                let pk_ph = dialect.placeholder_at(1);
                format!("DELETE FROM {} WHERE {} = {}", dialect.quote_ident(#table_name_lit), dialect.quote_ident(#pk_field_name_lit), pk_ph)
            }

            fn insert_params(&self) -> Vec<::prkdb_orm::types::SqlValue> {
                // Run validations first. If validation fails, we panic with a clear message.
                // (Table trait presently returns Vec for insert_params; if you'd rather a Result, change trait.)
                if let Err(err_msg) = self.validate_instance() {
                    panic!("Validation failed for insert on table '{}': {}", #table_name_lit, err_msg);
                }

                let mut v = Vec::new();
                #(#insert_push_tokens)*
                v
            }

            fn pk_to_params(pk: &Self::Id) -> Vec<::prkdb_orm::types::SqlValue> {
                vec![ ::prkdb_orm::types::SqlValue::from(pk) ]
            }
        }

        // Validation method injected on the struct
        impl #struct_ident {
            /// Validate this instance according to `#[validate(...)]` attributes on fields.
            ///
            /// EXPRESSION validators must reference `self`, e.g. `#[validate(self.age >= 18)]`.
            /// FUNCTION validators should be a function path accepting `&field` and returning `Result<_, _>`,
            /// e.g. `#[validate(check_first_name)]` expands to `check_first_name(&self.first_name)`.
            ///
            /// Returns `Ok(())` on success, otherwise `Err(String)` with a debug-friendly message.
            pub fn validate_instance(&self) -> Result<(), String> {
                #(#validate_checks)*
                Ok(())
            }
        }

        // TableHelpers
        impl ::prkdb_orm::schema::TableHelpers for #struct_ident {
            fn select() -> ::prkdb_orm::builders::SelectBuilder<Self> {
                ::prkdb_orm::builders::SelectBuilder::new()
            }
            fn insert() -> ::prkdb_orm::builders::InsertBuilder<Self> {
                ::prkdb_orm::builders::InsertBuilder::new()
            }
            fn update() -> ::prkdb_orm::builders::UpdateBuilder<Self> {
                ::prkdb_orm::builders::UpdateBuilder::new()
            }
            fn delete() -> ::prkdb_orm::builders::DeleteBuilder<Self> {
                ::prkdb_orm::builders::DeleteBuilder::new()
            }
        }

        impl ::prkdb_orm::schema::FromRow for #struct_ident {
            fn from_row(row: &::prkdb_orm::schema::Row) -> Result<Self, String> {
                Ok(Self {
                    #(#from_row_field_inits),*,
                    #(#join_defaults),*
                })
            }
        }

        impl ::prkdb_orm::schema::Relations for #struct_ident {
            fn foreign_keys() -> &'static [(&'static str, &'static str)] {
                &[#(#foreign_metadata_tokens),*]
            }
            fn joins() -> &'static [(&'static str, &'static str, &'static str)] {
                use std::sync::OnceLock;
                static JOINS: OnceLock<Vec<(&'static str, &'static str, &'static str)>> = OnceLock::new();
                JOINS.get_or_init(|| vec![#(#join_metadata_tokens),*]).as_slice()
            }
        }

        // Select trait & impl
        pub trait #select_trait_ident {
            #(#select_sigs)*
        }

        // Insert trait & impl
        pub trait #insert_trait_ident {
            #(#insert_sigs)*
        }

        impl #insert_trait_ident for ::prkdb_orm::builders::InsertBuilder<#struct_ident> {
            #(#insert_impls)*
        }

        impl #select_trait_ident for ::prkdb_orm::builders::SelectBuilder<#struct_ident> {
            #(#select_impls)*

            fn join(mut self, other: &str, on: &str) -> Self {
                self = ::prkdb_orm::builders::SelectBuilder::add_join(self, "INNER JOIN", other, on);
                self
            }
            fn left_join(mut self, other: &str, on: &str) -> Self {
                self = ::prkdb_orm::builders::SelectBuilder::add_join(self, "LEFT JOIN", other, on);
                self
            }
            fn right_join(mut self, other: &str, on: &str) -> Self {
                self = ::prkdb_orm::builders::SelectBuilder::add_join(self, "RIGHT JOIN", other, on);
                self
            }

            fn order_by(mut self, col: &str, asc: bool) -> Self {
                self = ::prkdb_orm::builders::SelectBuilder::add_order_by(self, col, asc);
                self
            }

            fn limit(self, n: usize) -> Self {
                ::prkdb_orm::builders::SelectBuilder::limit(self, n)
            }
            fn offset(self, n: usize) -> Self {
                ::prkdb_orm::builders::SelectBuilder::offset(self, n)
            }
            fn to_sql(self) -> (String, Vec<::prkdb_orm::types::SqlValue>) {
                ::prkdb_orm::builders::SelectBuilder::to_sql(self)
            }
        }

        // Update trait & impl
        pub trait #update_trait_ident {
            #(#update_sigs)*
        }

        impl #update_trait_ident for ::prkdb_orm::builders::UpdateBuilder<#struct_ident> {
            #(#update_impls)*

            fn to_sql(self) -> (String, Vec<::prkdb_orm::types::SqlValue>) {
                ::prkdb_orm::builders::UpdateBuilder::to_sql(self)
            }
        }

        // Delete trait & impl
        pub trait #delete_trait_ident {
            #(#delete_sigs)*
        }

        impl #delete_trait_ident for ::prkdb_orm::builders::DeleteBuilder<#struct_ident> {
            #(#delete_impls)*

            fn limit(self, n: usize) -> Self {
                ::prkdb_orm::builders::DeleteBuilder::limit(self, n)
            }
            fn offset(self, n: usize) -> Self {
                ::prkdb_orm::builders::DeleteBuilder::offset(self, n)
            }
            fn to_sql(self) -> (String, Vec<::prkdb_orm::types::SqlValue>) {
                ::prkdb_orm::builders::DeleteBuilder::to_sql(self)
            }
        }
    };

    TokenStream::from(expanded)
}
