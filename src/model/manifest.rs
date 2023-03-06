//! [manifest](https://iceberg.apache.org/spec/#manifests) and [partition](https://iceberg.apache.org/spec/#partition-specs) related structs

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use serde_with::serde_as;
use serde_with::Bytes;
use serde_with::DefaultOnNull;
use serde_with::{DeserializeAs, SerializeAs};

#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// helper struct to deserialize a map of i32 to bytes
struct BytesPair {
    key: i32,
    #[serde_as(as = "Bytes")]
    value: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// helper struct to deserialize a map of i32 to i64
struct NumPair {
    key: i32,
    value: i64,
}

impl SerializeAs<(i32, Vec<u8>)> for BytesPair {
    fn serialize_as<S>(source: &(i32, Vec<u8>), serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let p = BytesPair {
            key: source.0,
            value: source.1.clone(),
        };
        p.serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, (i32, Vec<u8>)> for BytesPair {
    fn deserialize_as<D>(deserializer: D) -> Result<(i32, Vec<u8>), D::Error>
    where
        D: Deserializer<'de>,
    {
        let pair = BytesPair::deserialize(deserializer)?;
        Ok((pair.key, pair.value))
    }
}

impl SerializeAs<(i32, i64)> for NumPair {
    fn serialize_as<S>(source: &(i32, i64), serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let p = NumPair {
            key: source.0,
            value: source.1,
        };
        p.serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, (i32, i64)> for NumPair {
    fn deserialize_as<D>(deserializer: D) -> Result<(i32, i64), D::Error>
    where
        D: Deserializer<'de>,
    {
        let pair = NumPair::deserialize(deserializer)?;
        Ok((pair.key, pair.value))
    }
}

/// A manifest list file storing ptrs to manifest avro files
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestList {
    /// location of manifest file
    pub manifest_path: String,
    /// length of the manifest file in bytes
    pub manifest_length: i64,
    /// id of a partition spec used to write the manifest
    /// must be listed in table metadata `partition-specs`
    pub partition_spec_id: i32,
    /// sequence number when the manifest was add to the table
    /// use 0 when reading manifest-format v1
    pub sequence_number: i32,
    /// int with meaning:
    /// - 0: data
    /// - 1: deletes
    pub content: i32,
}

/// Manifest Lists files store manifest_file
///
/// A manifest list includes summary metadata that
/// can be used to avoid scanning all of the manifests
/// in a snapshot when planning a table scan.
/// This includes the number of added, existing, and deleted files,
/// and a summary of values for each field of the partition spec used to write the manifest.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ManifestFile {
    /// location of the manifest file
    pub manifest_path: String,
    /// length of the manifest file in bytes
    pub manifest_length: i64,
    /// ID of the snapshot where the manifest file was added
    pub added_snapshot_id: i64,
    /// Number of entries in the manifest that have status `ADDED`;
    /// when `null` is assumed to be none zero
    pub added_files_count: Option<i32>,
    /// Number of entries in the manifest that have status `EXISTING`;
    /// when `null` this is assumed to be none zero
    pub existing_files_count: Option<i32>,
    /// Number of entries in the manifest that have status `DELETED`;
    /// when `null` this is assumed to be none zero
    pub deleted_fields_count: Option<i32>,
    /// A list of field summaries for each partition field in the spec.
    /// Each field in the list corresponds to a field in the manifest file's partition spec
    pub partitions: Vec<FieldSummary>,
    /// Number of rows in all the files in the manifest that have status `ADDED`;
    /// when `null` is assumed to be none zero
    pub added_rows_count: Option<i64>,
    /// Number of rows in all the files in the manifest that have status `EXISTING`;
    /// when `null` this is assumed to be none zero
    pub existing_rows_count: Option<i64>,
    /// Number of rows in all the files in the manifest that have status `DELETED`;
    /// when `null` this is assumed to be none zero
    pub deleted_rows_count: Option<i64>,
    /// ID of a partition spec used to write the manifest;
    /// must be listed in table metadata's `partition-specs`
    pub partition_spec_id: i32,
}

/// field summary of manifest list
#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FieldSummary {
    /// whether the manifest contains at least one partition with a null value
    /// for the field
    pub contains_null: bool,
    /// whether the manifest contains at least one partition with a nan value
    /// for the field
    pub contains_nan: bool,
    /// Lower bound for the non-null, non-NaN values in the partition field,
    /// or null if all values are null or NaN
    #[serde_as(as = "Bytes")]
    pub lower_bound: Vec<u8>,
    /// Upper bound for the non-null, non-NaN values in the partition field,
    /// or null if all values are null or NaN
    #[serde_as(as = "Bytes")]
    pub upper_bound: Vec<u8>,
}

/// A manifest is an immutable Avro file that
/// lists data files or delete files,
/// along with each file’s partition data tuple, metrics, and tracking information.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Manifest {
    /// JSON representation of the table schema at the time the manifest was written
    pub schema: ManifestEntry,
    /// id of the schema used to write the manifest as a string
    pub schema_id: i64,
    /// Json fields representation of the partition spec used to write the manifest
    pub partition_spec: Value,
    /// ID of the partition spec used to write the manifest as a string
    #[serde(default)]
    pub partition_spec_id: String,
    /// the version of format
    pub format_version: i64,
    /// a enum indicating diff type, add or sub
    pub content: String,
}

/// The manifest entry is a struct that contains the metadata of the file
#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// int with meaning:
    /// - 0: Existing
    /// - 1: ADDED
    /// - 2: DELETED
    pub status: i32,
    /// snapshot id where the file was added, or deleted if status is 2.
    /// inherited when null
    pub snapshot_id: Option<i64>,
    /// a struct containing file path, partition tuple, metrics...
    pub data_file: DataFile,
    /// data file sequence number
    /// inherited when null, and status is ADDED (1)
    pub sequence_number: Option<i64>,
    /// file sequence number indicating when the file is added
    pub file_sequence_number: Option<i64>,
}

/// the data file is a struct that contains the metadata of the file
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DataFile {
    /// path to the data file
    file_path: String,
    /// format of the data file, Avro, Orc or Parquet
    file_format: String,
    /// partition data tuple, schema based on the partition spec output using partition field ids
    /// for the struct field ids
    partition: Value,

    /// number of records in the file
    record_count: i64,
    /// total file size in bytes
    file_size_in_bytes: i64,
    /// Map from column id to the total size on disk of all regions that store the column.
    /// Does not include bytes necessary to read other columns, like footers. Leave null for row-oriented formats (Avro)
    #[serde(default)]
    #[serde_as(as = "DefaultOnNull<Vec<NumPair>>")]
    column_sizes: Vec<(i32, i64)>,
    /// Map from column ID to number of values in the column
    /// (NULL and NaN included)
    #[serde(default)]
    #[serde_as(as = "DefaultOnNull<Vec<NumPair>>")]
    value_counts: Vec<(i32, i64)>,
    /// Map from column id to number of null values in the column
    #[serde(default)]
    #[serde_as(as = "DefaultOnNull<Vec<NumPair>>")]
    null_value_counts: Vec<(i32, i64)>,
    /// Map from column id to number of distinct values in the column
    #[serde(default)]
    #[serde_as(as = "DefaultOnNull<Vec<NumPair>>")]
    distinct_counts: Vec<(i32, i64)>,
    /// map from column id to the minimum value in the column
    #[serde(default)]
    #[serde_as(as = "DefaultOnNull<Vec<BytesPair>>")]
    lower_bounds: Vec<(i32, Vec<u8>)>,
    /// map fro column id to the maximum value in the column
    #[serde(default)]
    #[serde_as(as = "DefaultOnNull<Vec<BytesPair>>")]
    upper_bounds: Vec<(i32, Vec<u8>)>,
    /// Implementation-specific key metadata for encryption
    #[serde(default)]
    #[serde_as(as = "DefaultOnNull<Bytes>")]
    key_metadata: Vec<u8>,
    /// Split offsets for the data file.
    /// For example, all row group offsets in a Parquet file. Must be sorted ascending
    #[serde(default)]
    split_offsets: Vec<i64>,
    /// Field ids used to determine row equality in equality delete files.
    /// required when content=2 and should be null otherwize.
    ///  Fields with ids listed in this column must be present in the delete file
    #[serde(default)]
    equality_ids: Vec<i32>,
    /// Type of content stored by the data file: data, equality deletes, or position deletes (all v1 files are data files)
    content: i32,
    /// Map from column id to number of nan values in the column
    #[serde(default)]
    #[serde_as(as = "DefaultOnNull<Vec<NumPair>>")]
    nan_value_counts: Vec<(i32, i64)>,
    /// ID representing sort order of this file
    sort_order_id: i32,
}

#[cfg(test)]
mod test {
    use anyhow::anyhow;
    use anyhow::Result;
    use apache_avro::from_value;

    use crate::model::manifest::Manifest;
    use crate::model::manifest::ManifestEntry;

    #[test]
    pub fn test_parse_manifest_lists() -> Result<()> {
        let manifest_list_path = "test-data/metadata/snap-6560075252320843098-1-9624c71f-198f-47fe-824b-0291f8998018.avro";
        let file = std::fs::File::open(manifest_list_path)?;
        let reader = apache_avro::Reader::new(file)?;
        for value in reader {
            let manifest_list: Manifest = from_value(&value?)
                .map_err(|e| anyhow!("failed to read manifest list: {:?}", e))?;
            println!("{:?}", manifest_list);
        }
        Ok(())
    }
    #[test]
    pub fn test_parse_manifest_entry() -> Result<()> {
        let manifest_path = "test-data/metadata/9624c71f-198f-47fe-824b-0291f8998018-m1.avro";
        let file = std::fs::File::open(manifest_path)?;
        let reader = apache_avro::Reader::new(file)?;
        for value in reader {
            let manifest: ManifestEntry =
                from_value(&value?).map_err(|e| anyhow!("failed to read manifest: {:?}", e))?;
            println!("{:?}", manifest);
        }
        Ok(())
    }
}
