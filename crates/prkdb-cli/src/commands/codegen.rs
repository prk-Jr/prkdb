//! Codegen command for generating cross-language SDK clients
//!
//! Usage:
//!   prkdb codegen --server http://localhost:50051 --lang python --out ./clients/python/
//!   prkdb codegen --server http://localhost:50051 --lang typescript --out ./clients/ts/
//!   prkdb codegen --server http://localhost:50051 --lang go --out ./clients/go/

use clap::{Args, ValueEnum};
use prkdb_client::PrkDbClient;
use prost::Message;
use prost_types::FileDescriptorSet;
use std::path::PathBuf;
use tokio::fs;

/// Supported output languages for code generation
#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
pub enum Language {
    Python,
    Typescript,
    Go,
    All,
}

#[derive(Args, Clone)]
pub struct CodegenArgs {
    /// gRPC server address used to fetch schemas.
    ///
    /// Use `http://127.0.0.1:8080` for `prkdb-server`, or
    /// `http://127.0.0.1:50051` for the local gRPC endpoint exposed by
    /// `prkdb-cli serve`.
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub server: String,

    /// Admin token used when listing schemas from a secured server
    #[arg(long, env = "PRKDB_ADMIN_TOKEN")]
    pub admin_token: Option<String>,

    /// Target language for code generation
    #[arg(short, long, value_enum)]
    pub lang: Language,

    /// Output directory for generated code
    #[arg(short, long)]
    pub out: PathBuf,

    /// Collection name (if omitted, generates for all schemas)
    #[arg(short, long)]
    pub collection: Option<String>,

    /// Overwrite existing files
    #[arg(long)]
    pub force: bool,
}

pub async fn handle_codegen(args: CodegenArgs) -> anyhow::Result<()> {
    println!("🔧 PrkDB Codegen");
    println!("   Server: {}", args.server);
    println!("   Language: {:?}", args.lang);
    println!("   Output: {}", args.out.display());

    // Connect to server
    let client = if let Some(token) = args.admin_token.clone() {
        PrkDbClient::new(vec![args.server.clone()])
            .await?
            .with_admin_token(token)
    } else {
        PrkDbClient::new(vec![args.server.clone()]).await?
    };
    println!("✓ Connected to server");

    // Create output directory
    fs::create_dir_all(&args.out).await?;

    // Fetch schemas from server
    let schemas = if let Some(collection) = &args.collection {
        // Fetch specific schema
        match client.get_schema(collection, None).await {
            Ok(schema) => vec![(collection.clone(), schema)],
            Err(e) => {
                eprintln!("Failed to fetch schema for '{}': {}", collection, e);
                return Err(e);
            }
        }
    } else {
        // Fetch all schemas
        let list = client.list_schemas().await?;
        let mut schemas = Vec::new();
        for info in list {
            match client.get_schema(&info.collection, None).await {
                Ok(schema) => schemas.push((info.collection, schema)),
                Err(e) => eprintln!(
                    "Warning: Failed to fetch schema for '{}': {}",
                    info.collection, e
                ),
            }
        }
        schemas
    };

    // Generate base client library (regardless of schemas)
    match args.lang {
        Language::Python => generate_python_client_lib(&args.out).await?,
        Language::Go => generate_go_client_lib(&args.out).await?,
        Language::All => {
            generate_python_client_lib(&args.out.join("python")).await?;
            generate_go_client_lib(&args.out.join("go")).await?;
        }
        _ => {}
    }

    if schemas.is_empty() {
        println!("No schemas found on server.");
        return Ok(());
    }

    println!("✓ Found {} schema(s)", schemas.len());

    // Generate code for each schema

    for (collection, schema) in &schemas {
        match args.lang {
            Language::Python => generate_python(&args.out, collection, schema).await?,
            Language::Typescript => generate_typescript(&args.out, collection, schema).await?,
            Language::Go => generate_go(&args.out, collection, schema).await?,
            Language::All => {
                generate_python(&args.out.join("python"), collection, schema).await?;
                generate_typescript(&args.out.join("typescript"), collection, schema).await?;
                generate_go(&args.out.join("go"), collection, schema).await?;
            }
        }
    }

    println!("✓ Code generation complete!");
    Ok(())
}

/// Parse schema descriptor bytes into message definitions
fn parse_schema_messages(schema_bytes: &[u8]) -> Vec<MessageInfo> {
    let mut messages = Vec::new();

    if let Ok(set) = FileDescriptorSet::decode(schema_bytes) {
        for file in set.file {
            let package = file.package.clone().unwrap_or_default();
            for user_msg in file.message_type {
                collect_messages(&user_msg, &package, &mut messages);
            }
        }
    } else {
        eprintln!("Warning: Failed to decode schema bytes as FileDescriptorSet");
    }

    messages
}

fn collect_messages(msg: &prost_types::DescriptorProto, _scope: &str, out: &mut Vec<MessageInfo>) {
    let name = msg.name.clone().unwrap_or_default();
    let fields = msg
        .field
        .iter()
        .map(|f| {
            let type_num = f.r#type.unwrap_or(0);
            let label = f.label.unwrap_or(1);
            let is_explicit_optional = f.proto3_optional.unwrap_or(false);
            let is_message = type_num == 11;

            FieldInfo {
                name: f.name.clone().unwrap_or_default(),
                _number: f.number.unwrap_or(0),
                proto_type: type_num,
                type_name: f.type_name.clone(), // Capture type name for messages
                is_optional: is_explicit_optional || is_message,
                is_repeated: label == 3,
            }
        })
        .collect();

    out.push(MessageInfo { name, fields });

    // Recurse into nested types
    for nested in &msg.nested_type {
        collect_messages(nested, _scope, out);
    }
}

struct MessageInfo {
    name: String,
    fields: Vec<FieldInfo>,
}

#[derive(Debug)]
struct FieldInfo {
    name: String,
    _number: i32,
    proto_type: i32,
    type_name: Option<String>,
    is_optional: bool,
    is_repeated: bool,
}

impl FieldInfo {
    fn python_type(&self) -> String {
        let base = match self.proto_type {
            1 => "float",         // Double
            2 => "float",         // Float
            3 | 17 | 18 => "int", // Int64, sint32, sint64
            4 | 6 => "int",       // Uint64, fixed64
            5 | 15 | 16 => "int", // Int32, sfixed32, sfixed64
            7 | 13 => "int",      // Fixed32, uint32
            8 => "bool",          // Bool
            9 => "str",           // String
            12 => "bytes",        // Bytes
            _ => {
                // Return class name for messages (strip path)
                if let Some(name) = &self.type_name {
                    name.split('.').next_back().unwrap_or("Any")
                } else {
                    "Any"
                }
            }
        };

        if self.is_repeated {
            format!("List[{}]", base)
        } else if self.is_optional {
            format!("Optional[{}]", base)
        } else {
            base.to_string()
        }
    }

    fn typescript_type(&self) -> String {
        let base = match self.proto_type {
            1 | 2 => "number",                // Double, Float
            3..=7 | 13 | 15..=18 => "number", // All ints
            8 => "boolean",                   // Bool
            9 => "string",                    // String
            12 => "Uint8Array",               // Bytes
            _ => {
                if let Some(name) = &self.type_name {
                    name.split('.').next_back().unwrap_or("any")
                } else {
                    "any"
                }
            }
        };

        if self.is_repeated {
            format!("{}[]", base)
        } else if self.is_optional {
            format!("{} | undefined", base)
        } else {
            base.to_string()
        }
    }

    fn go_type(&self) -> String {
        let base = match self.proto_type {
            1 => "float64",
            2 => "float32",
            3 | 18 => "int64",
            4 | 6 => "uint64",
            5 | 17 => "int32",
            7 | 13 => "uint32",
            8 => "bool",
            9 => "string",
            12 => "[]byte",
            15 => "int32",
            16 => "int64",
            _ => {
                if let Some(name) = &self.type_name {
                    name.split('.').next_back().unwrap_or("interface{}")
                } else {
                    "interface{}"
                }
            }
        };

        if self.is_repeated && base != "[]byte" {
            // For messages in Go, repeated usually means slice of pointers? Or structs?
            // PrkDB codegen simplicity: Slice of structs or pointers.
            // Let's assume slice of structs for now, or match base.
            format!("[]{}", base)
        } else if self.is_optional {
            format!("*{}", base)
        } else {
            base.to_string() // Structs are values
        }
    }
}

async fn generate_python(out_dir: &PathBuf, collection: &str, schema: &[u8]) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir).await?;

    let messages = parse_schema_messages(schema);

    let mut code = format!(
        r#""""
Generated PrkDB client model for {}
"""
from dataclasses import dataclass
from typing import Optional, List, Any
import json

try:
    from .prkdb_client import PrkDbClient, QueryBuilder
except ImportError:
    from prkdb_client import PrkDbClient, QueryBuilder

"#,
        collection
    );

    for msg in &messages {
        let class_name = &msg.name;
        code.push_str(&format!(
            r#"
@dataclass
class {}:
"#,
            class_name
        ));

        if msg.fields.is_empty() {
            code.push_str("    pass\n");
        } else {
            for field in &msg.fields {
                code.push_str(&format!("    {}: {}\n", field.name, field.python_type()));
            }
        }

        code.push_str(&format!(
            r#"
    @classmethod
    def from_dict(cls, data: dict) -> '{}':
"#,
            class_name
        ));

        // Generate nested object deserialization logic
        for field in &msg.fields {
            if field.proto_type == 11 {
                // TYPE_MESSAGE
                // Extract class name from type_name (e.g. ".models.Address" -> "Address")
                if let Some(type_name_full) = &field.type_name {
                    let type_name = type_name_full.split('.').next_back().unwrap_or("Any");
                    if type_name != "Any" {
                        let field_name = &field.name;
                        if field.is_repeated {
                            code.push_str(&format!(
                                r#"        if '{}' in data and data['{}']:
            data['{}'] = [{}.from_dict(x) for x in data['{}']]
"#,
                                field_name, field_name, field_name, type_name, field_name
                            ));
                        } else {
                            code.push_str(&format!(
                                r#"        if '{}' in data and data['{}']:
            data['{}'] = {}.from_dict(data['{}'])
"#,
                                field_name, field_name, field_name, type_name, field_name
                            ));
                        }
                    }
                }
            }
        }

        // Generate list of known fields for filtering
        let valid_keys_str = msg
            .fields
            .iter()
            .map(|f| format!("'{}'", f.name))
            .collect::<Vec<_>>()
            .join(", ");

        code.push_str(&format!(
            r#"        # Filter extra fields (like _key, _full_key)
        valid_keys = {{{}}}
        filtered_data = {{k: v for k, v in data.items() if k in valid_keys}}
        return cls(**filtered_data)

    def to_bytes(self) -> bytes:
        """Serialize to bytes for PrkDB storage"""
        return json.dumps(self.__dict__, default=lambda o: o.__dict__).encode('utf-8')
    
    @classmethod
    def from_bytes(cls, data: bytes) -> '{}':
        """Deserialize from PrkDB storage"""
        d = json.loads(data.decode('utf-8'))
        return cls.from_dict(d)

    @classmethod
    def select(cls) -> '{}QueryBuilder':
        """Start a fluent query builder for this model"""
        return {}QueryBuilder(cls)

class {}QueryBuilder(QueryBuilder):
    def __init__(self, model_cls):
        super().__init__(model_cls, "{}")
"#,
            valid_keys_str, class_name, class_name, class_name, class_name, collection
        ));

        // Generate query methods for each field
        for field in &msg.fields {
            code.push_str(&format!(
                r#"    def where_{}_eq(self, value) -> '{}QueryBuilder':
        return self.filter("{}", "=", value)
    
    def where_{}_neq(self, value) -> '{}QueryBuilder':
        return self.filter("{}", "!=", value)
    
    def where_{}_contains(self, value) -> '{}QueryBuilder':
        return self.filter("{}", "~", value)

"#,
                field.name,
                class_name,
                field.name,
                field.name,
                class_name,
                field.name,
                field.name,
                class_name,
                field.name
            ));
        }
    }

    // Helper to handle nested dicts during deserialization might be needed?
    // For simple dataclasses, `cls(**d)` works if nested fields are dicts and we rely on duck typing or post-init.
    // To properly deserialize nested dataclasses, we need a helper.
    // For now, keeping it simple (dicts).

    let filename = format!("{}.py", collection.to_lowercase());
    fs::write(out_dir.join(&filename), code).await?;
    println!("  → Generated {}", filename);

    Ok(())
}

async fn generate_typescript(
    out_dir: &PathBuf,
    collection: &str,
    schema: &[u8],
) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir).await?;

    let messages = parse_schema_messages(schema);

    let mut code = format!(
        r#"/**
 * Generated PrkDB client model for {}
 */
"#,
        collection
    );

    for msg in &messages {
        let class_name = &msg.name;
        code.push_str(&format!(
            r#"
export interface {} {{
"#,
            class_name
        ));

        for field in &msg.fields {
            let optional = if field.is_optional { "?" } else { "" };
            code.push_str(&format!(
                "  {}{}: {};\n",
                field.name,
                optional,
                field.typescript_type()
            ));
        }

        code.push_str(&format!(
            r#"}}

export const {}Meta = {{
  fromDict: (data: any): {} => {{
    return {{
      ...data,
      // Handle nested types if needed
    }};
  }},
  select: (client: PrkDbClient): {}QueryBuilder => {{
    return new {}QueryBuilder(client);
  }},
}};

export class {}QueryBuilder {{
    private client: PrkDbClient;
    private filters: string[] = [];
    private sortField: string | null = null;
    private _limit: number = 100;
    private _offset: number = 0;

    constructor(client: PrkDbClient) {{
        this.client = client;
    }}

    filter(field: string, op: string, value: any): this {{
        this.filters.push(`${{field}}${{op}}${{value}}`);
        return this;
    }}

    sort(field: string, desc: boolean = false): this {{
        this.sortField = `${{field}}:${{desc ? 'desc' : 'asc'}}`;
        return this;
    }}

    limit(limit: number): this {{
        this._limit = limit;
        return this;
    }}

    offset(offset: number): this {{
        this._offset = offset;
        return this;
    }}

    async execute(): Promise<{}[]> {{
        return this.client.list("{}", {{
            limit: this._limit,
            offset: this._offset,
            filter: this.filters.join(','),
            sort: this.sortField || undefined,
        }});
    }}
"#,
            class_name, class_name, class_name, class_name, class_name, class_name, collection
        ));

        // Generate fluent methods
        for field in &msg.fields {
            code.push_str(&format!(
                r#"
    where{}Eq(value: {}): this {{
        return this.filter("{}", "=", value);
    }}
"#,
                to_pascal_case(&field.name),
                field.typescript_type(),
                field.name
            ));
        }

        code.push_str("}\n"); // Close QueryBuilder
    }

    // Add PrkDbClient class with dynamic collection methods
    code.push_str(
        r#"
export class PrkDbClient {
    private host: string;

    constructor(host: string = "http://127.0.0.1:8080") {
        this.host = host.replace(/\/$/, "");
    }

    async list<T>(collection: string, options: { limit?: number, offset?: number, filter?: string, sort?: string } = {}): Promise<T[]> {
        const params = new URLSearchParams();
        if (options.limit) params.set("limit", options.limit.toString());
        if (options.offset) params.set("offset", options.offset.toString());
        if (options.filter) params.set("filter", options.filter);
        if (options.sort) params.set("sort", options.sort);

        const response = await fetch(`${this.host}/collections/${collection}/data?${params}`);
        if (!response.ok) {
            throw new Error(`Failed to list collection: ${response.status}`);
        }

        const data = await response.json();
        
        const result = data.data || {};
        if (result && result.data && Array.isArray(result.data)) {
            return result.data;
        }
        return Array.isArray(result) ? result : [];
    }

    async get<T>(collection: string, id: string): Promise<T | null> {
        const response = await fetch(`${this.host}/collections/${collection}/data/${id}`);
        if (response.status === 404) return null;
        if (!response.ok) {
            throw new Error(`Failed to get record: ${response.status}`);
        }
        const data = await response.json();
        return data.data || null;
    }

    async put(collection: string, data: any): Promise<void> {
        const response = await fetch(`${this.host}/collections/${collection}/data`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        if (!response.ok) {
            throw new Error(`Failed to put record: ${response.status}`);
        }
    }

    async delete(collection: string, id: string): Promise<void> {
        const response = await fetch(`${this.host}/collections/${collection}/data/${id}`, {
            method: 'DELETE'
        });
        if (!response.ok) {
            throw new Error(`Failed to delete record: ${response.status}`);
        }
    }
}
"#
    );

    let filename = format!("{}.ts", collection.to_lowercase());
    fs::write(out_dir.join(&filename), code).await?;
    println!("  → Generated {}", filename);

    Ok(())
}

async fn generate_go(out_dir: &PathBuf, collection: &str, schema: &[u8]) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir).await?;

    let messages = parse_schema_messages(schema);

    let mut code = format!(
        r#"// Generated PrkDB client model for {}
package models

import "encoding/json"
"#,
        collection
    );

    for msg in &messages {
        let struct_name = to_pascal_case(&msg.name);
        code.push_str(&format!(
            r#"
type {} struct {{
"#,
            struct_name
        ));

        for field in &msg.fields {
            let go_name = to_pascal_case(&field.name);
            code.push_str(&format!(
                "\t{} {} `json:\"{}\"`\n",
                go_name,
                field.go_type(),
                field.name
            ));
        }

        code.push_str(&format!(
            r#"}}

func (m *{}) ToBytes() ([]byte, error) {{
	return json.Marshal(m)
}}

func {}FromBytes(data []byte) (*{}, error) {{
	var m {}
	if err := json.Unmarshal(data, &m); err != nil {{
		return nil, err
	}}
	return &m, nil
}}
"#,
            struct_name, struct_name, struct_name, struct_name
        ));
    }

    let filename = format!("{}.go", collection.to_lowercase());
    fs::write(out_dir.join(&filename), code).await?;
    println!("  → Generated {}", filename);

    Ok(())
}

async fn generate_go_client_lib(out_dir: &PathBuf) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir).await?;

    let code = r#"// Generated PrkDB Go client library
package models

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// PrkDbClient is a lightweight HTTP client for PrkDB.
type PrkDbClient struct {
	Host   string
	Client *http.Client
}

// NewPrkDbClient creates a new PrkDB client.
func NewPrkDbClient(host string) *PrkDbClient {
	return &PrkDbClient{
		Host:   host,
		Client: &http.Client{},
	}
}

// ListOptions configures list queries.
type ListOptions struct {
	Limit  int
	Offset int
	Filter string
	Sort   string
}

// ListRaw returns raw JSON results from a collection.
func (c *PrkDbClient) ListRaw(collection string, opts ListOptions) ([]map[string]interface{}, error) {
	params := url.Values{}
	if opts.Limit > 0 {
		params.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Offset > 0 {
		params.Set("offset", strconv.Itoa(opts.Offset))
	}
	if opts.Filter != "" {
		params.Set("filter", opts.Filter)
	}
	if opts.Sort != "" {
		params.Set("sort", opts.Sort)
	}

	resp, err := c.Client.Get(fmt.Sprintf("%s/collections/%s/data?%s", c.Host, collection, params.Encode()))
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list failed with status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body failed: %w", err)
	}

	var wrapper struct {
		Data struct {
			Data []map[string]interface{} `json:"data"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		return nil, fmt.Errorf("unmarshal failed: %w", err)
	}

	return wrapper.Data.Data, nil
}

// Put inserts or updates a record in the collection.
func (c *PrkDbClient) Put(collection string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/collections/%s/data", c.Host, collection), bytes.NewReader(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("put failed with status %d", resp.StatusCode)
	}
	return nil
}

// Delete removes a record from the collection.
func (c *PrkDbClient) Delete(collection string, id string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/collections/%s/data/%s", c.Host, collection, id), nil)
	if err != nil {
		return err
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("delete failed with status %d", resp.StatusCode)
	}
	return nil
}
"#;

    fs::write(out_dir.join("client.go"), code).await?;
    println!("  → Generated client.go");

    Ok(())
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().chain(chars).collect::<String>(),
                None => String::new(),
            }
        })
        .collect()
}

async fn generate_python_client_lib(out_dir: &PathBuf) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir).await?;

    // Generate __init__.py
    fs::write(out_dir.join("__init__.py"), "").await?;

    // Generate prkdb_client.py
    let code = r#"
import json
import asyncio
from typing import Optional, List, Any, Dict, AsyncGenerator

try:
    import httpx
except ImportError:
    raise ImportError("The 'httpx' library is required. Please install it with: pip install httpx")

class PrkDbClient:
    def __init__(self, host: str = "http://127.0.0.1:8080"):
        self.host = host.rstrip('/')
        self.client = httpx.AsyncClient()
    
    async def close(self):
        await self.client.aclose()
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def list(self, collection: str, limit: int = 100, offset: int = 0, filter: Optional[str] = None, sort: Optional[str] = None) -> List[Dict[str, Any]]:
        params = {"limit": limit, "offset": offset}
        if filter:
            params["filter"] = filter
        if sort:
            params["sort"] = sort
            
        response = await self.client.get(f"{self.host}/collections/{collection}/data", params=params)
        
        if response.status_code == 200:
            data = response.json()
            # Response is wrapped in {"success": true, "data": ...}
            result = data.get("data", {})
            # For data endpoints, the actual list is wrapped in the result
            if isinstance(result, dict) and "data" in result and isinstance(result["data"], list):
                return result["data"]
            return result if isinstance(result, list) else []
        else:
            raise Exception(f"Failed to list collection: {response.status_code}")

    async def put(self, collection: str, data: Dict[str, Any]) -> None:
        """Insert or update a record in the collection"""
        response = await self.client.put(
            f"{self.host}/collections/{collection}/data",
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code not in (200, 201):
            raise Exception(f"Failed to put record: {response.status_code}")

    async def get(self, collection: str, id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single record by ID"""
        response = await self.client.get(f"{self.host}/collections/{collection}/data/{id}")
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise Exception(f"Failed to get record: {response.status_code}")

        data = response.json()
        return data.get("data")

    async def delete(self, collection: str, id: str) -> None:
        """Delete a record from the collection"""
        response = await self.client.delete(f"{self.host}/collections/{collection}/data/{id}")
        if response.status_code != 200:
            raise Exception(f"Failed to delete record: {response.status_code}")

    async def replay_collection(self, collection: str, handler):
        """
        Replay all events/items in a collection and apply them to a stateful handler.
        Uses streaming to avoid loading all data into memory.
        
        Handler must implement:
          - init_state(self) -> state
          - handle(self, state, event) -> void (modifies state in place)
        """
        limit = 100
        offset = 0
        state = handler.init_state()
        
        while True:
            items = await self.list(collection, limit=limit, offset=offset)
            if not items:
                break
            
            for item in items:
                 handler.handle(state, item)
            
            if len(items) < limit:
                break
                
            offset += len(items)
            
        return state
        
    async def stream(self, collection: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream all records from a collection using an async generator"""
        limit = 100
        offset = 0
        
        while True:
            items = await self.list(collection, limit=limit, offset=offset)
            if not items:
                break
                
            for item in items:
                yield item
                
            if len(items) < limit:
                break
                
            offset += len(items)

class QueryBuilder:
    def __init__(self, model_cls, collection_name: str):
        self.model_cls = model_cls
        self.collection_name = collection_name
        self.filters = []
        self.sort_field = None
        self._limit = 100
        self._offset = 0

    def filter(self, field, op, value):
        self.filters.append(f"{field}{op}{value}")
        return self
        
    def sort(self, field, desc=False):
        suffix = ":desc" if desc else ":asc"
        self.sort_field = f"{field}{suffix}"
        return self
        
    def limit(self, limit):
        self._limit = limit
        return self
        
    def offset(self, offset):
        self._offset = offset
        return self

    async def execute(self, client: PrkDbClient) -> List[Any]:
        items = await client.list(
            self.collection_name, 
            limit=self._limit, 
            offset=self._offset, 
            filter=",".join(self.filters) if self.filters else None,
            sort=self.sort_field
        )
        # Convert dicts to model objects
        return [self.model_cls.from_dict(item) for item in items]
"#;

    fs::write(out_dir.join("prkdb_client.py"), code).await?;
    println!("  → Generated prkdb_client.py");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use tempfile::tempdir;

    fn create_test_schema() -> Vec<u8> {
        let field_id = prost_types::FieldDescriptorProto {
            name: Some("id".to_string()),
            number: Some(1),
            r#type: Some(9), // TYPE_STRING
            ..Default::default()
        };
        let field_age = prost_types::FieldDescriptorProto {
            name: Some("age".to_string()),
            number: Some(2),
            r#type: Some(5), // TYPE_INT32
            ..Default::default()
        };
        let msg = prost_types::DescriptorProto {
            name: Some("User".to_string()),
            field: vec![field_id, field_age],
            ..Default::default()
        };
        let file = prost_types::FileDescriptorProto {
            name: Some("user.proto".to_string()),
            package: Some("test".to_string()),
            message_type: vec![msg],
            ..Default::default()
        };
        let set = prost_types::FileDescriptorSet { file: vec![file] };
        set.encode_to_vec()
    }

    #[tokio::test]
    async fn test_generate_python() {
        let dir = tempdir().unwrap();
        let schema = create_test_schema();
        generate_python(&dir.path().to_path_buf(), "user", &schema)
            .await
            .unwrap();

        let py_code = fs::read_to_string(dir.path().join("user.py"))
            .await
            .unwrap();
        assert!(py_code.contains("class User:"));
        assert!(py_code.contains("id: str"));
        assert!(py_code.contains("age: int"));
    }

    #[tokio::test]
    async fn test_generate_typescript() {
        let dir = tempdir().unwrap();
        let schema = create_test_schema();
        generate_typescript(&dir.path().to_path_buf(), "user", &schema)
            .await
            .unwrap();

        let ts_code = fs::read_to_string(dir.path().join("user.ts"))
            .await
            .unwrap();
        assert!(ts_code.contains("export interface User {"));
        assert!(ts_code.contains("id: string;"));
        assert!(ts_code.contains("age: number;"));
        assert!(ts_code.contains("export class UserQueryBuilder"));
        assert!(ts_code.contains(r#"constructor(host: string = "http://127.0.0.1:8080") {"#));
        assert!(ts_code.contains("headers: { 'Content-Type': 'application/json' },"));
        assert!(!ts_code.contains("export class PrkDbClient {{"));
        assert!(!ts_code.contains("headers: {{ 'Content-Type': 'application/json' }},"));
    }

    #[tokio::test]
    async fn test_generate_go() {
        let dir = tempdir().unwrap();
        let schema = create_test_schema();
        generate_go(&dir.path().to_path_buf(), "user", &schema)
            .await
            .unwrap();

        let go_code = fs::read_to_string(dir.path().join("user.go"))
            .await
            .unwrap();
        assert!(go_code.contains("type User struct {"));
        assert!(go_code.contains("Id string `json:\"id\"`"));
        assert!(go_code.contains("Age int32 `json:\"age\"`"));
    }
}
