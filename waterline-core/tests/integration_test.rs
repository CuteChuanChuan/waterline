use iceberg::spec::FormatVersion::V2;
use waterline_core::{MetadataError, load_table_metadata};

#[test]
fn test_read_table_metadata() {
    let path = "tests/fixtures/v2_partitioned_8_snapshots.metadata.json";
    let metadata = load_table_metadata(path).unwrap();

    assert_eq!(metadata.format_version(), V2);
    assert_eq!(metadata.snapshots().len(), 8);
}

#[test]
fn test_read_table_metadata_with_invalid_path() {
    let path = "tests/fixtures/invalid_path.metadata.json";
    let result = load_table_metadata(path);

    assert!(matches!(result, Err(MetadataError::NotFound { .. })));
}
