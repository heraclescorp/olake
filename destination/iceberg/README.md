# Iceberg-writer

Writes to apache-iceberg. This writer takes configuration of catalog and s3 buckets and writes data received from the Olake source.

## How It Works

The destination uses Apache Iceberg Go v0.7.0 and Arrow Go to write Parquet files and commit directly to the catalog. It does not launch a JVM or use a gRPC writer service.

The destination uses upstream `github.com/apache/iceberg-go v0.7.0` without patches or a local replacement. Known upstream default-handling bugs remain: JSON decoding can round integer defaults above 2^53, temporal defaults do not round-trip in the standard ISO representation, and projections containing only fields absent from an older file can fail with a zero-child-array error.

- Backfill appends data files.
- CDC supports equality deletes, positional deletes, and Iceberg v3 deletion vectors. Deletes retain OLake's soft-delete rows.
- Recovery metadata and data/delete files publish in the same catalog transaction. Positional/deletion-vector modes preserve the durable Pebble index, rebuild it from the committed snapshot when necessary, and recover unchanged PostgreSQL TOAST values.
- A catalog-scoped per-table coordinator serializes local metadata mutations and reuses the latest successful commit response. Normal Close does not reload metadata; definite conflicts refresh it, while ambiguous outcomes retain staged files and require catalog reopen/recovery. Every writer setup loads authoritative metadata for external schema changes and REST credentials. Active writers verify table identity even after cache invalidation or replacement.
- Each destination table must have one OLake process owner. The coordinator is not a distributed lock or a CAS on recovery properties; server snapshot/default-shape guards remain enabled for external data changes. Full-refresh file writes stay parallel, but table commits and index publication are serialized.
- S3 Tables manages removal of unreferenced objects. Superseded staged files are excluded from commits and left for that maintenance; rewrite, abort, and rejected-commit cleanup never issue object DELETEs to reserved `--table-s3` buckets. Other storage backends retain physical cleanup and propagate rewrite cleanup errors.
- Source-column changes honor `drop_removed_columns`, keep identifier/partition/sort source columns, and assign fresh IDs on drop/re-add. Version 3 uses upstream historical-default handling with the limitations above; version 2 logs a warning and leaves historical rows NULL rather than upgrading implicitly. Changed columns invalidate TOAST carry values; unchanged carry and deletion tombstones remain intact.
- REST (including S3 Tables and Unity), Glue, JDBC PostgreSQL/SQLite, Hive, and Hadoop catalogs use native adapters. S3, GCS, local files, and HDFS use native FileIO. Managed REST catalogs choose physical table locations; `iceberg_s3_path` remains the warehouse identifier for their catalog configuration.
- HTTP diagnostics emit one `iceberg_http` event per wire attempt with status, latency, request ID, and retry/conflict outcome. Credentials, query strings, headers, and bodies are omitted. REST throttling retries use cryptographically secure sub-10% jitter and remain bounded; ambiguous commit failures are not blindly replayed.

## Index Diagnostics

`native_index_reconcile` emits a `decision` and `complete` event per indexed table reconciliation, linked by `reconcile_id` within the connector process. Count `event=complete` by pod/table/mode/reason to measure frequency without double-counting decisions or confusing scan batches with separate rebuilds. Failures before planning emit a completion with the failing stage.

- `skip / up_to_date`: reuse the durable checkpoint without scanning files.
- `incremental / files_added`: index new files only; `snapshot_advanced_without_file_changes` advances only the checkpoint.
- `rebuild / missing_or_invalid_checkpoint`: no usable checkpoint; this alone does not distinguish deployment, lost storage, or an interrupted scan.
- `rebuild / files_removed`, `delete_sets_changed`, or `files_removed_and_delete_sets_changed`: changed physical file layout/deletes require rebuilding the table index.
- `error / checkpoint_not_ancestor`: an explicit reset is required; no automatic destructive reset occurs.

Events include old/target snapshots, the target snapshot's operation, file-delta counts, selected rows/bytes, scanned/indexed/skipped rows, completed files, flushes, checkpoint invalidation/publication, duration, and failure stage. Selected row/byte counts come from file metadata; indexed rows count entries staged, not distinct keys. The target operation is not proof of which engine performed maintenance. No row values, identifiers, credentials, or object paths are emitted.

## Build and Runtime

Use the repository's Go toolchain. `make dev.postgres.build` and the container build enable CGO and the `kerberos` build tag for Hive SASL. Linux Hive SASL also needs the MIT GSSAPI runtime (`libgssapi_krb5.so`); the runtime image supplies it. A direct build is:

```bash
CGO_ENABLED=1 go build -tags kerberos -o olake ./drivers/postgres
```

`GOMEMLIMIT` controls the Go heap budget. `OLAKE_INDEX_DB_DIR` selects the durable index directory. `OLAKE_TIMING=1` enables phase timing logs. No JDK, Maven, writer JAR, RPC port, or Arrow/legacy writer selector is required.

## How to run 

### Local Minio + JDBC Catalog (Local test setup):

Make sure you have docker installed before you run this

```shell
cd destination/iceberg/local-test
docker compose up
```
This will create 
1. postgres --> JDBC catalog
2. Minio --> For AWS S3 like filesystem setup on your local
3. Spark --> Querying Iceberg data.

Now create a writer.json for iceberg writer as follows : 
```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "jdbc",
    "jdbc_url": "jdbc:postgresql://localhost:5432/iceberg",
    "jdbc_username": "iceberg",
    "jdbc_password": "password",
    "iceberg_s3_path": "s3a://warehouse",
    "s3_endpoint": "http://localhost:9000",
    "s3_use_ssl": false,
    "s3_path_style": true,
    "aws_access_key": "admin",
    "aws_secret_key": "password"
  }
}  
```
And run the sync normally as mentioned in the getting started doc.

> Now how to see if the data is actually populated?
Run : 
```shell
# Connect to the spark-iceberg container
docker exec -it spark-iceberg bash

# Start spark-sql (rerun it if error occurs)
spark-sql

# Query in format select * from catalog_name.iceberg_db_name.table_name
select * from olake_iceberg.olake_iceberg.table_name;
```


### AWS S3 + Glue Catalog
Create a json for writer config (Works for S3 as storage and AWS Glue as a catalog) : 
```json
{
    "type": "ICEBERG",
    "writer": {
      "iceberg_s3_path": "s3://bucket_name/olake_iceberg/test_olake",
      "aws_region": "ap-south-1",
      "aws_access_key": "XXX",
      "aws_secret_key": "XXX"
    }
  }  
```

And run the sync normally as mentioned in the getting started doc.

* `iceberg_s3_path` -> Stores the relevant iceberg data/metadata files
* `aws_region` -> Region for AWS bucket and catalog
* `aws_access_key` -> AWS access key which has full access to glue & AWS S3
* `aws_secret_key` -> AWS secret key

### REST Catalog
Create a json for writer config (writer.json)
```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "rest",
    "rest_catalog_url": "http://localhost:8181/catalog",
    "iceberg_s3_path": "warehouse"
  }
}
```

**Authentication Fields (optional):**
- `token` - Bearer token for token-based authentication
- `oauth2_uri` - OAuth2 server URI for OAuth2 authentication
- `rest_auth_type` - Authentication type (e.g., "oauth2")
- `credential` - Client secret or credential for OAuth2 (Usually id:secret)
- `scope` - OAuth2 scopes (space-separated)
- `rest_signing_name` - Service name for AWS Signature V4 (e.g., "s3tables")
- `rest_signing_region` - Region for AWS Signature V4 signing
- `rest_signing_v_4` - Enable AWS Signature V4 signing (boolean)

### S3 Table Bucket
Create a json for writer config (writer.json)
```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "rest",
    "rest_catalog_url": "https://s3tables.us-east-1.amazonaws.com/iceberg",
    "iceberg_s3_path": "arn:aws:s3tables:<REGION>:<ACCOUNT_ID>:bucket/<BUCKET_NAME>",
    "aws_access_key": "",
    "aws_secret_key": "",
    "aws_region": "<REGION>",
    "rest_signing_name": "s3tables",
    "rest_signing_region": "<REGION>",
    "rest_signing_v_4": true
  }
}
```

change the placeholders with actual values
* `REGION` -> Region for AWS bucket and catalog
* `NAMESPACE` -> This will be your s3 table bucket namespace
* `ACCOUNT_ID` -> AWS account identifier
* `BUCKET_NAME` -> Table Bucket Name

### Unity Catalog support (Rest)
```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "rest",
    "normalization": true,
    "rest_catalog_url": "https://<DATABRICK_WORKSPACE_URL>/api/2.1/unity-catalog/iceberg-rest",
    "iceberg_s3_path": "<CATALOG_NAME>",
    "token": "<DATABRICK_USER_PERSONAL_ACCESS_TOKEN>",
    "no_identifier_fields": true
  }
}
```

change the placeholders with actual values
* `DATABRICK_WORKSPACE_URL` -> Databricks workplace URL (URL that you used to access your Databricks console)
* `CATALOG_NAME` -> Catalog name (ex, workspace)
* `NAMESPACE` -> Namespace name inside catalog (ex, default)
* `DATABRICK_USER_PERSONAL_ACCESS_TOKEN` -> Go to settings > developer > create PAT
* no_identifier_fields -> true (This is needed for environments which doesn't support Equality delete based updates. Ex Databricks Unity managed iceberg tables)

Note : Auth can be done using Oauth2 as well.

### Hive Catalog
Create a json for writer config (writer.json)
```json
{
    "type": "ICEBERG",
    "writer": {
        "catalog_type": "hive",
        "iceberg_s3_path": "s3a://warehouse/",
        "aws_region": "us-east-1",
        "aws_access_key": "admin",
        "aws_secret_key": "password",
        "s3_endpoint": "http://localhost:9000",
        "hive_uri": "http://localhost:9083",
        "s3_use_ssl": false,
        "s3_path_style": true,
        "hive_clients": 5,
        "hive_sasl_enabled": false
    }
}
```

To use Iceberg with GCP, configure the writer to connect to a Hive Metastore hosted on Dataproc Metastore and write data to Dataproc generated bucket
```json
{
  "type": "ICEBERG",
  "writer": {
      "catalog_type": "hive",
      "hive_uri": "thrift://<hive-dataproc-metastore-ip>:9083",
      "hive_clients": 10,
      "hive_sasl_enabled": false,
      "iceberg_s3_path": "gs://gcp-Dataproc-bucket/hive-warehouse",
      "aws_region": "us-central1"
  }
}
```

Please change the above to real credentials to make it work.

For detailed catalog configs and usage, refer [here.](https://olake.io/docs/category/catalogs)

## Partitioning Support

The Iceberg writer supports partitioning data based on field values, which can significantly improve query performance when filtering on partition columns. Partitioning is configured at the stream level using the `PartitionRegex` field in `StreamMetadata`.

### Partition Configuration Format

Partitions are specified using the following format:

```
/{field_name, transform}/{another_field, transform}
```

Where:
- `field_name`: Name of the column to partition by
- `transform`: Iceberg partition transform to apply (e.g., `identity`, `hour`, `day`, `month`, `year`, `bucket[N]`, `truncate[N]`)

### Example Partition Configurations

1. Partition by year from a timestamp column:
```
/{created_at, year}
```

2. Partition by day:
```
/{event_date, day}
```

3. Multiple partitions (by customer ID and month):
```
/{customer_id, identity}/{event_time, month}
```

4. Using a current timestamp (special case):
```
/{now(), day}
```

### Supported Transforms

The Iceberg writer supports the following transforms:
- `identity`: Use the raw value (good for categorical data)
- `year`, `month`, `day`, `hour`: Time-based transforms (good for timestamp columns)
- `bucket[N]`: Hash the value into N buckets (for high-cardinality fields)
- `truncate[N]`: Truncate the string to N characters (for string fields)

For more details on partition transforms, see the [Iceberg Partition Transform Specification](https://iceberg.apache.org/spec/#partition-transforms).

### Handling Missing Partition Fields

When a partition field is missing from a record, the writer will automatically set the field to `nil`, which Iceberg treats as a null value. This ensures that records with missing partition fields can still be processed correctly.

### Example Usage

To include partitioning in your sync **streams.json**:

1. Specify the partitioning in your stream configuration:
```json
{
  "selected_streams": {
    "my_namespace": [
      {
        "stream_name": "my_stream",
        "partition_regex": "/{timestamp_col, day}/{region, identity}"
      }
    ]
  }
}
```
2. Run your sync as usual, and the Iceberg writer will create the appropriate partitioned table structure.

After syncing, you can query the data efficiently by filtering on partition columns:

```sql
-- This query will only scan relevant partitions
select * from olake_iceberg.olake_iceberg.my_stream 
where timestamp_col = '2023-05-01' and region = 'us-east';
```