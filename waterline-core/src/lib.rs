//! Core library: Iceberg/Parquet metadata parsing + health metrics.

use iceberg::spec::TableMetadata;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("file not found: {path}")]
    NotFound { path: PathBuf },
    #[error("failed to read {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid table metadata JSON: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn parse_table_metadata(json: &str) -> Result<TableMetadata, MetadataError> {
    Ok(serde_json::from_str(json)?)
}

pub fn load_table_metadata(path: impl AsRef<Path>) -> Result<TableMetadata, MetadataError> {
    let path = path.as_ref();
    let content = std::fs::read_to_string(path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => MetadataError::NotFound { path: path.into() },
        _ => MetadataError::Io {
            path: path.into(),
            source: e,
        },
    })?;
    parse_table_metadata(&content)
}
