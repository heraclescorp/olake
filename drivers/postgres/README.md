# Postgres Driver
The Postgres Driver enables data synchronization from Postgres to your desired destination. It supports both **Full Refresh** and **CDC (Change Data Capture)** modes.

---

## Supported Modes
1. **Full Refresh**
   Fetches the complete dataset from Postgres.
2. **CDC (Change Data Capture)**
   Tracks and syncs incremental changes from Postgres in real time.
3. **Strict CDC (Change Data Capture)**
   Tracks only new changes from the current position in the PostgreSQL WAL, without performing an initial backfill.
4. **Incremental**
   Fetches and syncs changes which have cursor value greater than or equal to the saved position.

---

## Setup and Configuration
To run the Postgres Driver, configure the following files with your specific credentials and settings:

- **`config.json`**: postgres connection details.
- **`streams.json`**: List of collections and fields to sync (generated using the *Discover* command).
- **`write.json`**: Configuration for the destination where the data will be written.

Place these files in your project directory before running the commands.

### Config File
Add Postgres credentials in following format in `config.json` file. [More details.](https://olake.io/docs/connectors/postgres/config)
   ```json
   {
    "host": "postgres-host",
    "port": 5432,
    "database": "postgres_db",
    "username": "postgres_user",
    "password": "postgres_pass",
    "jdbc_url_params": {},
    "ssl": {
        "mode": "disable"
    },
    "update_method": {
        "replication_slot": "postgres_slot",
        "initial_wait_time":120
    },
    "reader_batch_size": 100000,
    "max_threads" :50,
    "retry_count" :2,
    "ssh_config":{
         "host": "ssh_host",
         "port": 22,
         "username": "ssh_user",
         "private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nssh_passkey\n-----END OPENSSH PRIVATE KEY-----"
    }
  }
```


## Commands

### Discover Command

The *Discover* command generates json content for `streams.json` file, which defines the schema of the collections to be synced.

#### Usage
To run the Discover command, use the following syntax
   ```bash
   ./build.sh driver-postgres discover --config /postgres/examples/config.json
   ```

#### Example Response (Formatted)
After executing the Discover command, a formatted response will look like this:
```json
{
  "type": "CATALOG",
  "catalog": {
      "selected_streams": {
         "public": [
               {
                  "partition_regex": "",
                  "stream_name": "table_1",
                  "chunk_column":"",
                  "normalization": false,
                  "append_mode": false,
                  "filter": "id > 1"
               }
         ]
      },
      "streams": [
         {
         "stream": {
            "name": "table_1",
            "namespace": "public",
            ...
         }
         }
      ]
  }
}
```

#### Configure Streams
Before running the Sync command, the generated `streams.json` file must be configured. Follow these steps:
- Remove Unnecessary Streams:<br>
   Remove streams from selected streams.
- Add Partition based on Column Value
   Modify partition_regex field to partition destination data based on column value
- Add split column (primary key) based on which full load chunks can be created

- Modify Each Stream:<br>
   For each stream you want to sync:<br>
   - Add the following properties:
      ```json
      "sync_mode": "cdc",
      ```
   - Specify the cursor field (only for incremental syncs):
      ```json
      "cursor_field": "<cursor field from available_cursor_fields>"
      ```
   - To enable `append_mode` mode, explicitly set it to `true` in the selected stream configuration. \
      Similarly, for `chunk_column`, ensure it is defined in the stream settings as required.
      ```json
         "selected_streams": {
            "public": [
                  {
                     "partition_regex": "",
                     "stream_name": "table_1",
                     "chunk_column":"",         //column name to be specified
                     "normalization": false,
                     "append_mode": false
                  }
            ]
         },
      ```
   - The `filter` mode under selected_streams allows you to define precise criteria for selectively syncing data from your source.
      ```json
         "selected_streams": {
            "namespace": [
                  {
                     "partition_regex": "",
                     "stream_name": "table_1",
                     "normalization": false,
                     "filter": "id > 1 and created_at <= \"2025-05-27T11:43:40.497+00:00\""
                  }
            ]
         },
      ```
   - Add `cursor_field` from set of `available_cursor_fields` in case of incremental sync. This column will be used to track which rows from the table must be synced. If the primary cursor field is expected to contain `null` values, a fallback cursor field can be specified after the primary cursor field using a colon separator. The system will use the fallback cursor when the primary cursor is `null`.
        > **Note**: For incremental sync to work correctly, the primary cursor field (and fallback cursor field if defined) must contain at least one non-null value. Defined cursor fields cannot be entirely null.
      ```json
         "sync_mode": "incremental",
         "cursor_field": "UPDATED_AT:CREATED_AT" // UPDATED_AT is the primary cursor field, CREATED_AT is the fallback cursor field (which can be skipped if the primary cursor is not expected to contain null values)
      ```

- Final Streams Example
<br> `normalization` determines that level 1 flattening is required. <br>
<br> The `append_mode` flag determines whether records can be written to the iceberg delete file. If set to true, no records will be written to the delete file. Know more about delete file: [Iceberg MOR and COW](https://olake.io/iceberg/mor-vs-cow)<br>
   ```json
   {
      "selected_streams": {
         "public": [
               {
                  "partition_regex": "",
                  "stream_name": "table_1",
                  "normalization": false,
                  "append_mode": false
               }
         ]
      },
      "streams": [
         {
            "stream": {
               "name": "table_1",
               "namespace": "public",
               ...
               "sync_mode": "cdc"
            }
         }
      ]
   }
   ```

### Writer File
The Writer file defines the configuration for the destination where data needs to be added.<br>
Example (For Local):
   ```
   {
      "type": "PARQUET",
      "writer": {
         "local_path": "./examples/reader"
      }
   }
   ```
Example (For S3):
   ```
   {
      "type": "PARQUET",
      "writer": {
         "s3_bucket": "olake",
         "s3_region": "",
         "s3_access_key": "",
         "s3_secret_key": "",
         "s3_path": ""
      }
   }
   ```

Example (For AWS S3 + Glue Configuration)
  ```
  {
      "type": "ICEBERG",
      "writer": {
        "s3_path": "s3://{bucket_name}/{path_prefix}/",
        "aws_access_key": "XXX",
        "aws_secret_key": "XXX",
        "database": "olake_iceberg",
        "grpc_port": 50051,
        "server_host": "localhost"
      }
  }
  ```

Example (Local Test Configuration (JDBC + Minio))
  ```
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

Find more about writer docs [here.](https://olake.io/docs/category/destinations-writers)

### Sync Command
The *Sync* command fetches data from Postgres and ingests it into the destination.

```bash
./build.sh driver-postgres sync --config /postgres/examples/config.json --catalog /postgres/examples/streams.json --destination /postgres/examples/write.json
```

To run sync with state
```bash
./build.sh driver-postgres sync --config /postgres/examples/config.json --catalog /postgres/examples/streams.json --destination /postgres/examples/write.json --state /postgres/examples/state.json
```


### State File
The State file is generated by the CLI command at the completion of a batch or the end of a sync. This file can be used to save the sync progress and later resume from a specific checkpoint.
#### State File Format
You can save the state in a `state.json` file using the following format:
```json
{
    "type": "GLOBAL",
    "global": {
        "state": {
            "lsn": "2D9/AD00445A"
        },
        "streams": [
            "public.table_1",
            "public.table_2"
        ]
    },
    "streams": [
        {
            "stream": "table_1",
            "namespace": "public",
            "sync_mode": "",
            "state": {
                "chunks": []
            }
        },
        {
            "stream": "table_2",
            "namespace": "public",
            "sync_mode": "",
            "state": {
                "chunks": []
            }
        }
    ]
}
```

Find more at [Postgres Docs](https://olake.io/docs/category/postgres)
### Source-based TOAST recovery with equality deletes (S3 Tables)

For a latest-state replica, avoid rebuilding an identifier-to-file/position index
when managed S3 Tables compaction rewrites files:

1. In the PostgreSQL source's `update_method`, opt in explicitly:

   ```json
   {
     "type": "CDC",
     "replication_slot": "your_existing_slot",
     "publication": "your_existing_publication",
     "toast_value_source": "source"
   }
   ```

   Keep the existing update-method type spelling and other settings in your job.
   TOAST preservation is mandatory. Omitting `toast_value_source` selects
   source-based recovery, including for existing configurations without that field.
   Select `"toast_value_source": "destination"` explicitly to keep indexed
   destination recovery (requiring `pos` or `dv`). The legacy
   `preserve_toast_values` flag is ignored, including when set to false; it can no
   longer disable recovery. Existing explicitly configured `"destination"` values are not overridden.
   Default source recovery requires primary-key SELECT access on the CDC writer
   and has the current-value and missing-row behavior described below.

2. Rediscover streams so `available_update_types` reflects source recovery and
   your actual downstream query engines. In **each selected table's stream
   metadata** set `"update_type": "eq"` and `"append_mode": false`. For example,
   merge these fields into the existing `selected_streams` entry (do not replace
   the rest of the catalog):

   ```json
   {"stream_name": "analytics_event", "update_type": "eq", "append_mode": false}
   ```

   Equality mode does not open a physical row index, so it skips index bootstrap
   and compaction-driven index reconciliation. Source recovery also works with
   the existing indexed modes, but those still incur index maintenance.

3. Deploy the new connector before changing a running job's settings. Switch at
   a stopped, committed sync boundary; retain the job's source state and slot.
   Do **not** reset/recreate the destination table merely to switch `dv` to `eq`.
   Existing v2 tables stay v2; existing v3 tables stay v3, keep existing DVs, and
   accept new equality deletes. Same-write positional removals use DVs on v3.
   The `dv` → `eq` catalog transition no longer requests a table clear.

**No PostgreSQL `ALTER TABLE ... REPLICA IDENTITY FULL`, Iceberg format downgrade,
`write.delete.mode` property change, or S3 maintenance change is required.** Keep
managed compaction enabled. The replication role needs SELECT permission on the
primary-key and recovered columns. The configured CDC `host` must provide fresh
source reads (normally the primary/writer); the optional `backfill_host` is never
used for hydration. Source recovery rejects standby CDC hosts because their reads are not replay-fenced.
All downstream readers must support equality deletes **and** the existing table
format/DVs. The query-engine discovery restrictions remain enforced; for example,
do not select equality for readers that only support positional deletes/DVs.

#### Semantics and performance

- This is **not event-time/audit-accurate reconstruction**. Ordinary source SELECTs
  can return values newer than the WAL event, including values beyond a bounded
  recovery/resync endpoint. Snapshot history records the hydrated rows OLake
  actually wrote, not guaranteed source row images at those LSNs.
- Only columns explicitly marked unavailable are filled. Explicit WAL values,
  SQL NULLs, binary data, and ordinary full-image events retain their meanings.
- Lookups are parameterized primary-key queries, grouped by table and missing
  column projection, deduplicated within each group, with at most 128 buffered
  events per batch (or 4 MiB of WAL payload before hydration). Large TOAST values
  can exceed that payload bound; it is not a hard process-memory bound. Queries
  execute serially with a 30-second timeout and caller cancellation.
- Output stays in WAL order. Pending events flush before schema notifications,
  at the batch bound, and after successful WAL completion. Failed hydration or
  downstream writes fail the sync before its normal durable acknowledgement.
- Missing source rows (including rows deleted or rekeyed since the event) fail
  closed instead of silently inserting NULLs or skipping an update. A persistent
  missing row can block catch-up and requires reconciliation/resync or using
  destination-based recovery/full source row images. No automatic skip policy
  is implemented. Query failures can be retried by normal sync retry handling.
- The feature adds SELECT load and large-value transfer on affected source keys.
  Benchmark source load and query latency with real TOAST-heavy traffic before
  broad rollout. Equality deletes also add reader work until compaction applies
  them. Partition changes remain handled by the existing equality writer, which
  scopes deletes across live partitions and historical partition specs.
