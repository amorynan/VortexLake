//! Schema definitions for VortexLake tables
//!
//! Provides type-safe schema definitions that map to Arrow data types
//! with additional metadata for vector operations.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
// use serde::{Deserialize, Serialize};

/// A field in a VortexLake table schema
#[derive(Debug, Clone)]
pub struct Field {
    /// Field name
    pub name: String,
    /// Arrow data type
    pub data_type: DataType,
    /// Whether the field can be null
    pub nullable: bool,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl Field {
    /// Create a new field
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the field
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Check if this is a vector field
    pub fn is_vector(&self) -> bool {
        matches!(
            self.data_type,
            DataType::FixedSizeList(_, _) | DataType::List(_) | DataType::LargeList(_)
        )
    }

    /// Get vector dimension if this is a vector field
    pub fn vector_dimension(&self) -> Option<usize> {
        match &self.data_type {
            DataType::FixedSizeList(field, size) => {
                if field.data_type() == &DataType::Float32 {
                    Some(*size as usize)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Schema definition for a VortexLake table
#[derive(Debug, Clone)]
pub struct Schema {
    /// Table fields
    pub fields: Vec<Field>,
    /// Schema metadata
    pub metadata: HashMap<String, String>,
}

impl Schema {
    /// Create a new schema from fields
    pub fn new(fields: Vec<Field>) -> Result<Self> {
        // Validate field names are unique
        let mut names = std::collections::HashSet::new();
        for field in &fields {
            if !names.insert(&field.name) {
                return Err(anyhow!("Duplicate field name: {}", field.name));
            }
        }

        Ok(Self {
            fields,
            metadata: HashMap::new(),
        })
    }

    /// Create schema from Arrow schema
    pub fn from_arrow(arrow_schema: &ArrowSchema) -> Self {
        let fields = arrow_schema
            .fields()
            .iter()
            .map(|field| Field {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                metadata: field.metadata().clone(),
            })
            .collect();

        Self {
            fields,
            metadata: arrow_schema.metadata().clone(),
        }
    }

    /// Convert to Arrow schema
    pub fn to_arrow(&self) -> ArrowSchema {
        let fields: Vec<Arc<ArrowField>> = self
            .fields
            .iter()
            .map(|field| {
                Arc::new(ArrowField::new(
                    &field.name,
                    field.data_type.clone(),
                    field.nullable,
                ).with_metadata(field.metadata.clone()))
            })
            .collect();
            
        ArrowSchema::new_with_metadata(fields, self.metadata.clone())
    }

    /// Get field by name
    pub fn get_field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.name == name)
    }

    /// Get vector fields
    pub fn vector_fields(&self) -> Vec<&Field> {
        self.fields.iter().filter(|field| field.is_vector()).collect()
    }

    /// Validate a record batch against this schema
    pub fn validate_batch(&self, batch: &arrow::record_batch::RecordBatch) -> Result<()> {
        let arrow_schema = self.to_arrow();
        if batch.schema() != Arc::new(arrow_schema) {
            return Err(anyhow!("Record batch schema does not match table schema"));
        }
        Ok(())
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

impl From<&ArrowSchema> for Schema {
    fn from(arrow_schema: &ArrowSchema) -> Self {
        Self::from_arrow(arrow_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn test_vector_field_detection() {
        let vector_field = Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(ArrowField::new("item", DataType::Float32, false)),
                384,
            ),
            false,
        );

        assert!(vector_field.is_vector());
        assert_eq!(vector_field.vector_dimension(), Some(384));

        let text_field = Field::new("text", DataType::Utf8, false);
        assert!(!text_field.is_vector());
        assert_eq!(text_field.vector_dimension(), None);
    }

    #[test]
    fn test_schema_creation() {
        let fields = vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("vector", DataType::FixedSizeList(
                Arc::new(ArrowField::new("item", DataType::Float32, false)),
                768,
            ), false),
        ];

        let schema = Schema::new(fields).unwrap();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.vector_fields().len(), 1);
    }
}
