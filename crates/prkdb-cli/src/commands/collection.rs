use crate::collection_metadata::get_or_create_collection_metadata;
use crate::commands::CollectionCommands;
use crate::database_manager::scan_storage;
use crate::output::{display_single, info, success, OutputDisplay};
use crate::Cli;
use anyhow::Result;
use prkdb_storage_sled::SledAdapter;
use serde::Serialize;
use tabled::Tabled;

#[derive(Tabled, Serialize)]
struct CollectionInfo {
    name: String,
    items: u64,
    size_bytes: u64,
    partitions: u32,
}

#[derive(Serialize)]
struct CollectionDetails {
    name: String,
    items: u64,
    size_bytes: u64,
    partitions: u32,
    partition_config: Option<String>,
    created_at: String,
    schema_info: SchemaInfo,
}

#[derive(Serialize)]
struct SchemaInfo {
    detected_fields: Vec<FieldInfo>,
    primary_key: Option<String>,
    constraints: Vec<String>,
    data_types: std::collections::HashMap<String, String>,
    nullable_fields: Vec<String>,
}

#[derive(Serialize)]
struct FieldInfo {
    name: String,
    data_type: String,
    nullable: bool,
    sample_values: Vec<String>,
}

use prkdb_client::PrkDbClient;

pub async fn execute(cmd: CollectionCommands, cli: &Cli) -> Result<()> {
    match cmd {
        CollectionCommands::Create { name } => create_collection(&name, cli).await,
        CollectionCommands::Drop { name } => drop_collection(&name, cli).await,
        CollectionCommands::List => list_collections(cli).await,

        // Legacy commands requiring local DB access
        CollectionCommands::Describe { name } => {
            crate::init_database_manager(&cli.database);
            describe_collection(&name, cli).await
        }
        CollectionCommands::Count { name } => {
            crate::init_database_manager(&cli.database);
            count_collection(&name, cli).await
        }
        CollectionCommands::Sample { name, limit } => {
            crate::init_database_manager(&cli.database);
            sample_collection(&name, limit, cli).await
        }
        CollectionCommands::Data {
            name,
            limit,
            offset,
            filter,
            sort,
        } => {
            crate::init_database_manager(&cli.database);
            browse_collection_data(&name, limit, offset, filter, sort, cli).await
        }
    }
}

async fn create_collection(name: &str, cli: &Cli) -> Result<()> {
    let client = PrkDbClient::new(vec![cli.server.clone()]).await?;
    let client = if let Some(token) = &cli.admin_token {
        client.with_admin_token(token)
    } else {
        client
    };

    client.create_collection(name).await?;
    success(&format!("Collection '{}' created successfully", name));
    Ok(())
}

async fn drop_collection(name: &str, cli: &Cli) -> Result<()> {
    let client = PrkDbClient::new(vec![cli.server.clone()]).await?;
    let client = if let Some(token) = &cli.admin_token {
        client.with_admin_token(token)
    } else {
        client
    };

    client.drop_collection(name).await?;
    success(&format!("Collection '{}' dropped successfully", name));
    Ok(())
}

async fn list_collections(cli: &Cli) -> Result<()> {
    let client = PrkDbClient::new(vec![cli.server.clone()]).await?;
    let client = if let Some(token) = &cli.admin_token {
        client.with_admin_token(token)
    } else {
        client
    };

    let collections = client.list_collections().await?;

    if collections.is_empty() {
        info("No collections found.");
    } else {
        match cli.format {
            crate::OutputFormat::Table => {
                // Construct table-compatible output or just list
                let mut rows = Vec::new();
                for name in collections {
                    rows.push(CollectionInfo {
                        name,
                        items: 0, // Remote list doesn't return count yet
                        size_bytes: 0,
                        partitions: 1, // Placeholder
                    });
                }
                rows.display(cli)?;
            }
            _ => {
                // Json/Yaml
                let output = serde_json::json!({
                    "collections": collections
                });
                println!("{}", serde_json::to_string_pretty(&output)?);
            }
        }
    }

    Ok(())
}

async fn describe_collection(name: &str, cli: &Cli) -> Result<()> {
    info(&format!("Describing collection: {}", name));

    // Get all collections by scanning storage directly (same method as list_collections)
    let all_entries_result = scan_storage().await;

    let result = match all_entries_result {
        Ok(all_entries) => {
            // Group entries by collection type prefix to discover real collections
            let mut collection_found = false;
            let mut items = 0u64;
            let mut size_bytes = 0u64;
            let mut sample_data = Vec::new();

            for (key, value) in &all_entries {
                let key_str = String::from_utf8_lossy(key);

                // Handle both formats: "collection:id" and "collection::Type:id"
                let collection_name = if key_str.contains("::") {
                    // Format: "collection::Type:id" -> extract "collection"
                    key_str.split(':').next()
                } else {
                    // Format: "collection:id" -> extract "collection"
                    key_str.split(':').next()
                };

                if let Some(collection_type) = collection_name {
                    if collection_type == name {
                        collection_found = true;
                        items += 1;
                        size_bytes += (key.len() + value.len()) as u64;

                        // Collect sample data for schema analysis (up to 10 samples)
                        if sample_data.len() < 10 {
                            // Try multiple deserialization approaches

                            // 1. Try JSON first (for backward compatibility)
                            if let Ok(json_value) =
                                serde_json::from_slice::<serde_json::Value>(value)
                            {
                                sample_data.push(json_value);
                            }
                            // 2. Try bincode deserialization to JSON-compatible value
                            else if let Ok(json_value) = try_bincode_to_json(value) {
                                sample_data.push(json_value);
                            }
                            // 3. Try to create a synthetic JSON object from the key structure
                            else {
                                // Create a basic object with key information
                                let mut obj = serde_json::Map::new();
                                obj.insert(
                                    "_key".to_string(),
                                    serde_json::Value::String(key_str.to_string()),
                                );
                                obj.insert(
                                    "_data_format".to_string(),
                                    serde_json::Value::String("binary".to_string()),
                                );
                                obj.insert(
                                    "_size_bytes".to_string(),
                                    serde_json::Value::Number(serde_json::Number::from(
                                        value.len(),
                                    )),
                                );

                                // Try to extract ID from key
                                if let Some(id_part) = key_str.split(':').next_back() {
                                    obj.insert(
                                        "_id".to_string(),
                                        serde_json::Value::String(id_part.to_string()),
                                    );
                                }

                                sample_data.push(serde_json::Value::Object(obj));
                            }
                        }
                    }
                }
            }

            if collection_found {
                // Get collection metadata for creation time
                let created_at = if let Ok(storage) = SledAdapter::open(&cli.database) {
                    match get_or_create_collection_metadata(&storage, name).await {
                        Ok(metadata) => metadata.format_created_at(),
                        Err(_) => "Unknown".to_string(),
                    }
                } else {
                    "Unknown".to_string()
                };

                // Analyze schema from sample data
                let schema_info = analyze_schema(&sample_data);

                let details = CollectionDetails {
                    name: name.to_string(),
                    items,
                    size_bytes,
                    partitions: 1, // Single partition for now
                    partition_config: Some("DefaultPartitioner".to_string()),
                    created_at,
                    schema_info,
                };
                Ok(Some(details))
            } else {
                Ok(None)
            }
        }
        Err(e) => Err(e),
    };

    match result {
        Ok(Some(details)) => {
            display_single(&details, cli)?;
        }
        Ok(None) => {
            info(&format!("Collection '{}' not found", name));
        }
        Err(e) => {
            info(&format!("Unable to describe collection '{}': {}", name, e));
            info("This may happen when the database is busy. Please try again in a moment.");
        }
    }

    Ok(())
}

/// Analyze schema from sample JSON data
fn analyze_schema(sample_data: &[serde_json::Value]) -> SchemaInfo {
    if sample_data.is_empty() {
        return SchemaInfo {
            detected_fields: Vec::new(),
            primary_key: None,
            constraints: Vec::new(),
            data_types: std::collections::HashMap::new(),
            nullable_fields: Vec::new(),
        };
    }

    let mut field_types: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    let mut field_samples: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    let mut field_null_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut all_fields: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Analyze each sample
    for sample in sample_data {
        if let Some(obj) = sample.as_object() {
            // Track all fields in this sample
            let sample_fields: std::collections::HashSet<String> = obj.keys().cloned().collect();
            all_fields.extend(sample_fields.clone());

            // Check for missing fields (nulls)
            for field in &all_fields {
                if !sample_fields.contains(field) {
                    *field_null_counts.entry(field.clone()).or_insert(0) += 1;
                }
            }

            // Analyze each field in this sample
            for (key, value) in obj {
                let type_name = match value {
                    serde_json::Value::Null => {
                        *field_null_counts.entry(key.clone()).or_insert(0) += 1;
                        "null"
                    }
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Number(n) => {
                        if n.is_i64() {
                            "integer"
                        } else {
                            "number"
                        }
                    }
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::Object(_) => "object",
                };

                field_types
                    .entry(key.clone())
                    .or_default()
                    .insert(type_name.to_string());

                // Store sample values (limit to avoid too much data)
                if let Some(sample_str) = value.as_str() {
                    if sample_str.len() <= 50 {
                        // Only store short string samples
                        field_samples
                            .entry(key.clone())
                            .or_default()
                            .insert(sample_str.to_string());
                    }
                } else if !value.is_null() {
                    let value_str = value.to_string();
                    if value_str.len() <= 50 {
                        field_samples
                            .entry(key.clone())
                            .or_default()
                            .insert(value_str);
                    }
                }
            }
        }
    }

    // Build field info
    let mut detected_fields = Vec::new();
    let mut data_types = std::collections::HashMap::new();
    let mut nullable_fields = Vec::new();
    let mut primary_key = None;
    let mut constraints = Vec::new();

    for field in &all_fields {
        let types = field_types.get(field).cloned().unwrap_or_default();
        let null_count = field_null_counts.get(field).cloned().unwrap_or(0);
        let is_nullable = null_count > 0 || types.contains("null");

        // Determine primary data type
        let primary_type = if types.len() == 1 {
            types.iter().next().unwrap().clone()
        } else if types.contains("null") && types.len() == 2 {
            // If only null and one other type, use the other type
            types
                .iter()
                .find(|&t| t != "null")
                .unwrap_or(&"mixed".to_string())
                .clone()
        } else {
            "mixed".to_string()
        };

        // Get sample values
        let samples: Vec<String> = field_samples
            .get(field)
            .map(|set| set.iter().take(3).cloned().collect())
            .unwrap_or_default();

        // Detect potential primary key (id fields that appear in all samples)
        if (field == "id" || field.ends_with("_id"))
            && null_count == 0
            && (primary_type == "string" || primary_type == "integer")
        {
            primary_key = Some(field.clone());
        }

        detected_fields.push(FieldInfo {
            name: field.clone(),
            data_type: primary_type.clone(),
            nullable: is_nullable,
            sample_values: samples,
        });

        data_types.insert(field.clone(), primary_type);

        if is_nullable {
            nullable_fields.push(field.clone());
        }
    }

    // Sort fields for consistent output
    detected_fields.sort_by(|a, b| a.name.cmp(&b.name));
    nullable_fields.sort();

    // Add basic constraints based on analysis
    if let Some(ref pk) = primary_key {
        constraints.push(format!("PRIMARY KEY ({})", pk));
    }

    // Add NOT NULL constraints for non-nullable fields
    for field in &all_fields {
        if !nullable_fields.contains(field) && Some(field) != primary_key.as_ref() {
            constraints.push(format!("NOT NULL ({})", field));
        }
    }

    SchemaInfo {
        detected_fields,
        primary_key,
        constraints,
        data_types,
        nullable_fields,
    }
}

async fn count_collection(name: &str, cli: &Cli) -> Result<()> {
    info(&format!("Counting items in collection: {}", name));

    // Get all collections by scanning storage directly
    let all_entries_result = scan_storage().await;

    let result = match all_entries_result {
        Ok(all_entries) => {
            let mut collection_found = false;
            let mut count = 0u64;

            for (key, _) in &all_entries {
                let key_str = String::from_utf8_lossy(key);

                // Handle both formats: "collection:id" and "collection::Type:id"
                let collection_name = if key_str.contains("::") {
                    // Format: "collection::Type:id" -> extract "collection"
                    key_str.split(':').next()
                } else {
                    // Format: "collection:id" -> extract "collection"
                    key_str.split(':').next()
                };

                if let Some(collection_type) = collection_name {
                    if collection_type == name {
                        collection_found = true;
                        count += 1;
                    }
                }
            }

            if collection_found {
                Ok(Some(count))
            } else {
                Ok(None)
            }
        }
        Err(e) => Err(e),
    };

    match result {
        Ok(Some(count)) => match cli.format {
            crate::OutputFormat::Table => {
                success(&format!("Collection '{}' contains {} items", name, count));
            }
            _ => {
                let result = serde_json::json!({
                    "collection": name,
                    "count": count
                });
                println!("{}", serde_json::to_string_pretty(&result)?);
            }
        },
        Ok(None) => {
            info(&format!("Collection '{}' not found", name));
        }
        Err(e) => {
            info(&format!("Unable to count collection '{}': {}", name, e));
            info("This may happen when the database is busy. Please try again in a moment.");
        }
    }

    Ok(())
}

async fn sample_collection(name: &str, limit: usize, cli: &Cli) -> Result<()> {
    info(&format!(
        "Sampling {} items from collection: {}",
        limit, name
    ));

    // Get all collections by scanning storage directly
    let all_entries_result = scan_storage().await;

    let result = match all_entries_result {
        Ok(all_entries) => {
            let mut collection_entries = Vec::new();

            for (key, value) in &all_entries {
                let key_str = String::from_utf8_lossy(key);

                // Handle both formats: "collection:id" and "collection::Type:id"
                let collection_name = if key_str.contains("::") {
                    // Format: "collection::Type:id" -> extract "collection"
                    key_str.split(':').next()
                } else {
                    // Format: "collection:id" -> extract "collection"
                    key_str.split(':').next()
                };

                if let Some(collection_type) = collection_name {
                    if collection_type == name {
                        // Try multiple deserialization approaches
                        let mut json_value = None;

                        // 1. Try JSON first (for backward compatibility)
                        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(value) {
                            json_value = Some(value);
                        }
                        // 2. Try bincode deserialization
                        else if let Ok(value) = try_bincode_to_json(value) {
                            json_value = Some(value);
                        }

                        if let Some(value) = json_value {
                            collection_entries.push(value);
                            if collection_entries.len() >= limit {
                                break;
                            }
                        }
                    }
                }
            }

            if collection_entries.is_empty() {
                // Check if collection exists at all
                let collection_exists = all_entries.iter().any(|(key, _)| {
                    let key_str = String::from_utf8_lossy(key);
                    let collection_name = key_str.split(':').next();
                    collection_name == Some(name)
                });

                if collection_exists {
                    Ok(Some(Vec::new())) // Collection exists but no valid JSON data
                } else {
                    Ok(None) // Collection doesn't exist
                }
            } else {
                Ok(Some(collection_entries))
            }
        }
        Err(e) => Err(e),
    };

    match result {
        Ok(Some(samples)) => {
            if samples.is_empty() {
                info(&format!("No data found in collection '{}'", name));
                return Ok(());
            }

            match cli.format {
                crate::OutputFormat::Table => {
                    for (i, sample) in samples.iter().enumerate() {
                        println!("{}: {}", i + 1, serde_json::to_string_pretty(sample)?);
                    }
                }
                _ => {
                    let result = serde_json::json!({
                        "collection": name,
                        "limit": limit,
                        "samples": samples
                    });
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
            }
        }
        Ok(None) => {
            info(&format!("Collection '{}' not found", name));
        }
        Err(e) => {
            info(&format!("Unable to sample collection '{}': {}", name, e));
            info("This may happen when the database is busy. Please try again in a moment.");
        }
    }

    Ok(())
}

async fn browse_collection_data(
    name: &str,
    limit: usize,
    offset: usize,
    filter: Option<String>,
    sort: Option<String>,
    cli: &Cli,
) -> Result<()> {
    info(&format!(
        "Browsing data in collection: {} (limit: {}, offset: {})",
        name, limit, offset
    ));

    // Get all collections by scanning storage directly
    let all_entries_result = scan_storage().await;

    let result = match all_entries_result {
        Ok(all_entries) => {
            let mut collection_entries = Vec::new();

            // Collect all entries for this collection
            for (key, value) in &all_entries {
                let key_str = String::from_utf8_lossy(key);

                // Handle both formats: "collection:id" and "collection::Type:id"
                let collection_name = if key_str.contains("::") {
                    // Format: "collection::Type:id" -> extract "collection"
                    key_str.split(':').next()
                } else {
                    // Format: "collection:id" -> extract "collection"
                    key_str.split(':').next()
                };

                if let Some(collection_type) = collection_name {
                    if collection_type == name {
                        // Try multiple deserialization approaches
                        let mut entry = None;

                        // 1. Try JSON first (for backward compatibility)
                        if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(value) {
                            entry = Some(json_value);
                        }
                        // 2. Try bincode deserialization
                        else if let Ok(json_value) = try_bincode_to_json(value) {
                            entry = Some(json_value);
                        }

                        if let Some(mut json_entry) = entry {
                            let key_part = if key_str.contains("::") {
                                // Extract ID from "collection::Type:id" format
                                key_str.split(':').next_back().unwrap_or("unknown")
                            } else {
                                // Extract ID from "collection:id" format
                                key_str.split(':').nth(1).unwrap_or("unknown")
                            };

                            if let Some(obj) = json_entry.as_object_mut() {
                                obj.insert(
                                    "_key".to_string(),
                                    serde_json::Value::String(key_part.to_string()),
                                );
                                obj.insert(
                                    "_full_key".to_string(),
                                    serde_json::Value::String(key_str.to_string()),
                                );
                            }
                            collection_entries.push((key_str.to_string(), json_entry));
                        }
                    }
                }
            }

            if collection_entries.is_empty() {
                return Ok(());
            }

            // Apply filter if specified
            if let Some(filter_expr) = &filter {
                collection_entries = apply_simple_filter(collection_entries, filter_expr)?;
            }

            // Apply sort if specified
            if let Some(sort_field) = &sort {
                collection_entries = apply_simple_sort(collection_entries, sort_field)?;
            }

            // Apply pagination
            let total_count = collection_entries.len();
            let paginated_entries: Vec<_> = collection_entries
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect();

            if paginated_entries.is_empty() {
                info(&format!(
                    "No data found in collection '{}' for the given criteria",
                    name
                ));
                return Ok(());
            }

            match cli.format {
                crate::OutputFormat::Table => {
                    // Show pagination info
                    info(&format!(
                        "Showing {} items (offset: {}, total: {})",
                        paginated_entries.len(),
                        offset,
                        total_count
                    ));

                    // Display each entry
                    for (i, (key, entry)) in paginated_entries.iter().enumerate() {
                        println!(
                            "\n[{}] Key: {}",
                            offset + i + 1,
                            key.split(':').nth(1).unwrap_or("unknown")
                        );
                        println!("{}", serde_json::to_string_pretty(entry)?);

                        if i < paginated_entries.len() - 1 {
                            println!("---");
                        }
                    }

                    // Show navigation hints
                    if offset + limit < total_count {
                        info(&format!(
                            "Use --offset {} to see next {} items",
                            offset + limit,
                            limit
                        ));
                    }
                    if offset > 0 {
                        info(&format!(
                            "Use --offset {} to see previous items",
                            offset.saturating_sub(limit)
                        ));
                    }
                }
                _ => {
                    let result = serde_json::json!({
                        "collection": name,
                        "total_count": total_count,
                        "limit": limit,
                        "offset": offset,
                        "filter": filter,
                        "sort": sort,
                        "data": paginated_entries.iter().map(|(_, entry)| entry).collect::<Vec<_>>()
                    });
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
            }

            Ok(())
        }
        Err(e) => Err(e),
    };

    match result {
        Ok(_) => {}
        Err(e) => {
            info(&format!("Unable to browse collection '{}': {}", name, e));
            info("This may happen when the database is busy. Please try again in a moment.");
        }
    }

    Ok(())
}

/// Apply simple filter to collection entries
fn apply_simple_filter(
    entries: Vec<(String, serde_json::Value)>,
    filter_expr: &str,
) -> Result<Vec<(String, serde_json::Value)>> {
    // Simple filter format: "field=value" or "field!=value" or "field~value" (contains)
    let filtered = if let Some(eq_pos) = filter_expr.find("!=") {
        let (field, value) = filter_expr.split_at(eq_pos);
        let value = &value[2..]; // Skip "!="
        entries
            .into_iter()
            .filter(|(_, entry)| {
                entry
                    .get(field)
                    .and_then(|v| v.as_str())
                    .map(|s| s != value)
                    .unwrap_or(true)
            })
            .collect()
    } else if let Some(eq_pos) = filter_expr.find('=') {
        let (field, value) = filter_expr.split_at(eq_pos);
        let value = &value[1..]; // Skip "="
        entries
            .into_iter()
            .filter(|(_, entry)| {
                entry
                    .get(field)
                    .and_then(|v| v.as_str())
                    .map(|s| s == value)
                    .unwrap_or(false)
            })
            .collect()
    } else if let Some(tilde_pos) = filter_expr.find('~') {
        let (field, value) = filter_expr.split_at(tilde_pos);
        let value = &value[1..]; // Skip "~"
        entries
            .into_iter()
            .filter(|(_, entry)| {
                entry
                    .get(field)
                    .and_then(|v| v.as_str())
                    .map(|s| s.contains(value))
                    .unwrap_or(false)
            })
            .collect()
    } else {
        entries // No valid filter format, return all
    };

    Ok(filtered)
}

/// Apply simple sort to collection entries
fn apply_simple_sort(
    mut entries: Vec<(String, serde_json::Value)>,
    sort_field: &str,
) -> Result<Vec<(String, serde_json::Value)>> {
    let (field, descending) = if let Some(field) = sort_field.strip_suffix(":desc") {
        (field, true)
    } else if let Some(field) = sort_field.strip_suffix(":asc") {
        (field, false)
    } else {
        (sort_field, false) // Default to ascending
    };

    entries.sort_by(|(_, a), (_, b)| {
        let a_val = a.get(field);
        let b_val = b.get(field);

        let cmp = match (a_val, b_val) {
            (Some(a), Some(b)) => {
                // Try string comparison first
                if let (Some(a_str), Some(b_str)) = (a.as_str(), b.as_str()) {
                    a_str.cmp(b_str)
                }
                // Try number comparison
                else if let (Some(a_num), Some(b_num)) = (a.as_f64(), b.as_f64()) {
                    a_num
                        .partial_cmp(&b_num)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
                // Fallback to string representation
                else {
                    a.to_string().cmp(&b.to_string())
                }
            }
            (Some(_), None) => std::cmp::Ordering::Less, // Non-null values come first
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };

        if descending {
            cmp.reverse()
        } else {
            cmp
        }
    });

    Ok(entries)
}

/// Try to deserialize bincode data to JSON for schema analysis
pub fn try_bincode_to_json(data: &[u8]) -> Result<serde_json::Value> {
    // Since we don't know the exact type, we can try common patterns
    // For now, let's try to deserialize as a generic serde_json::Value
    // This is a simplified approach - in a real implementation,
    // we'd need type information or more sophisticated introspection

    // Try deserializing as various common types and convert to JSON

    // First, try to see if it's a simple string
    if let Ok(s) = bincode::deserialize::<String>(data) {
        return Ok(serde_json::Value::String(s));
    }

    // Try i64
    if data.len() == 8 {
        if let Ok(n) = bincode::deserialize::<i64>(data) {
            return Ok(serde_json::json!(n));
        }
    }

    // Try f64
    if data.len() == 8 {
        if let Ok(n) = bincode::deserialize::<f64>(data) {
            return Ok(serde_json::json!(n));
        }
    }

    // Try bool
    if data.len() == 1 {
        if let Ok(b) = bincode::deserialize::<bool>(data) {
            return Ok(serde_json::Value::Bool(b));
        }
    }

    // If all else fails, try to decode as a generic serde_json::Value
    // This might work if the original data was JSON-compatible
    if let Ok(value) = bincode::deserialize::<serde_json::Value>(data) {
        return Ok(value);
    }

    // Try to extract readable strings from the binary data
    // This is a heuristic approach to make binary data more readable
    let readable_parts = extract_readable_strings(data);
    if !readable_parts.is_empty() {
        let mut obj = serde_json::Map::new();

        // Try to identify common patterns in User-like structures
        if readable_parts.len() >= 2 {
            // Likely a User struct with id, name, email, age, city, is_active
            let mut field_index = 0;
            let field_names = ["id", "name", "email", "age", "city", "is_active"];

            for part in readable_parts.iter() {
                if field_index < field_names.len() {
                    // Skip very short strings that might be noise
                    if part.len() >= 2 {
                        obj.insert(
                            field_names[field_index].to_string(),
                            serde_json::Value::String(part.clone()),
                        );
                        field_index += 1;
                    }
                }
            }
        }

        // Add metadata
        obj.insert(
            "_data_format".to_string(),
            serde_json::Value::String("bincode_parsed".to_string()),
        );
        obj.insert(
            "_size_bytes".to_string(),
            serde_json::Value::Number(serde_json::Number::from(data.len())),
        );

        if !obj.is_empty() {
            return Ok(serde_json::Value::Object(obj));
        }
    }

    // Last resort: create a metadata object with hex representation
    let hex_data = data
        .iter()
        .take(32) // Limit to first 32 bytes
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("");

    let mut obj = serde_json::Map::new();
    obj.insert(
        "_bincode_hex".to_string(),
        serde_json::Value::String(hex_data),
    );
    obj.insert(
        "_size".to_string(),
        serde_json::Value::Number(serde_json::Number::from(data.len())),
    );
    obj.insert(
        "_note".to_string(),
        serde_json::Value::String("Binary data - could not parse as known types".to_string()),
    );

    // Add readable strings if any were found
    let readable_parts = extract_readable_strings(data);
    if !readable_parts.is_empty() {
        obj.insert(
            "_readable_strings".to_string(),
            serde_json::json!(readable_parts),
        );
    }

    Ok(serde_json::Value::Object(obj))
}

/// Extract readable ASCII strings from binary data
fn extract_readable_strings(data: &[u8]) -> Vec<String> {
    let mut strings = Vec::new();
    let mut current_string = Vec::new();

    for &byte in data {
        // Check if byte is printable ASCII (space to tilde)
        if (32..=126).contains(&byte) {
            current_string.push(byte);
        } else {
            // End of string - save if it's long enough to be meaningful
            if current_string.len() >= 3 {
                if let Ok(s) = String::from_utf8(current_string.clone()) {
                    strings.push(s);
                }
            }
            current_string.clear();
        }
    }

    // Don't forget the last string if the data ends with printable chars
    if current_string.len() >= 3 {
        if let Ok(s) = String::from_utf8(current_string) {
            strings.push(s);
        }
    }

    strings
}
