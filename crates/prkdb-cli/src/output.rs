use crate::{Cli, OutputFormat};
use colored::*;
use serde::Serialize;
use tabled::{Table, Tabled};

pub trait OutputDisplay {
    fn display(&self, cli: &Cli) -> anyhow::Result<()>;
}

impl<T> OutputDisplay for Vec<T>
where
    T: Tabled + Serialize,
{
    fn display(&self, cli: &Cli) -> anyhow::Result<()> {
        match cli.format {
            OutputFormat::Table => {
                if self.is_empty() {
                    println!("{}", "No items found".yellow());
                } else {
                    let table = Table::new(self);
                    println!("{}", table);
                }
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(self)?);
            }
            OutputFormat::Yaml => {
                // For simplicity, use JSON format for YAML too
                // In production, you'd use serde_yaml
                println!("{}", serde_json::to_string_pretty(self)?);
            }
        }
        Ok(())
    }
}

// Separate impl for single items that are NOT vectors
impl OutputDisplay for serde_json::Value {
    fn display(&self, cli: &Cli) -> anyhow::Result<()> {
        match cli.format {
            OutputFormat::Table => {
                // For single items, just use JSON-like output
                println!("{}", serde_json::to_string_pretty(self)?);
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(self)?);
            }
            OutputFormat::Yaml => {
                println!("{}", serde_json::to_string_pretty(self)?);
            }
        }
        Ok(())
    }
}

pub fn success(message: &str) {
    println!("{} {}", "✓".green(), message);
}

pub fn error(message: &str) {
    eprintln!("{} {}", "✗".red(), message);
}

pub fn info(message: &str) {
    println!("{} {}", "ℹ".blue(), message);
}

pub fn warning(message: &str) {
    println!("{} {}", "⚠".yellow(), message);
}

pub fn display_single<T: Serialize>(item: &T, cli: &Cli) -> anyhow::Result<()> {
    let json_value = serde_json::to_value(item)?;
    json_value.display(cli)
}
