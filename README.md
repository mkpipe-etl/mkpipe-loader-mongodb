# mkpipe-loader-mongodb

MongoDB loader plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Writes Spark DataFrames into MongoDB collections using the official MongoDB Spark Connector.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  mongo_target:
    variant: mongodb
    mongo_uri: 'mongodb://user:password@host:27017/mydb?authSource=admin'
    database: mydb
```

Alternatively, use individual parameters (URI is constructed automatically):

```yaml
connections:
  mongo_target:
    variant: mongodb
    host: localhost
    port: 27017
    database: mydb
    user: myuser
    password: mypassword
```

---

## Table Configuration

```yaml
pipelines:
  - name: pg_to_mongo
    source: pg_source
    destination: mongo_target
    tables:
      - name: public.events
        target_name: stg_events
        replication_method: full
```

---

## Write Strategy

Control how data is written to MongoDB:

```yaml
      - name: public.events
        target_name: stg_events
        write_strategy: upsert       # append | replace | upsert
        write_key: [event_id]        # required for upsert
```

| Strategy | MongoDB Behavior |
|---|---|
| `append` | Insert documents via Spark connector (default for incremental) |
| `replace` | Drop collection, then insert (default for full). Use `if_exists: append` to preserve existing collection |
| `upsert` | Auto-creates a unique index on `write_key` columns, then writes with Spark connector `operationType=replace` matching on `write_key` |

> **Note:** `upsert` requires `write_key`. The loader automatically creates a unique compound index on the `write_key` columns before writing. Existing documents matching the key are replaced; new documents are inserted.
>
> **Migration:** If you previously used `dedup_columns` for implicit upsert behavior, switch to explicit `write_strategy: upsert` with `write_key`. The old behavior still works but emits a deprecation warning.

---

## Write Parallelism

By default Spark writes to MongoDB using however many partitions the DataFrame currently has. You can control write parallelism with `write_partitions`, which calls `coalesce` before the write to reduce the number of open connections to MongoDB:

```yaml
      - name: public.events
        target_name: stg_events
        replication_method: full
        write_partitions: 4     # coalesce DataFrame to N partitions before writing
```

### When to use `write_partitions`

- **Reduce connections:** MongoDB has a connection limit per node. If Spark has many executors, each partition opens its own connection. Lowering `write_partitions` reduces connection count.
- **Increase throughput:** A small number of large batches is generally faster than many small batches. A value of 4–8 is a good starting point.
- **`coalesce` vs `repartition`:** `coalesce` avoids a shuffle (preferred for write). If the source has very few partitions and you want to increase them, use `repartition` — but that requires a code-level change, not a YAML setting.

### Performance Notes

- Write speed is mostly limited by MongoDB's write capacity and network, not Spark.
- `write_partitions` is most effective when reducing an already-large partition count.
- For append-mode incremental loads the default partition count is usually fine.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Source table/collection name |
| `target_name` | string | required | MongoDB destination collection name |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `write_partitions` | int | — | Coalesce DataFrame to N partitions before writing |
| `batchsize` | int | `10000` | Records per write batch |
| `write_strategy` | string | — | `append`, `replace`, `upsert` |
| `write_key` | list | — | Key columns for upsert (unique index created automatically) |
| `if_exists` | string | — | `replace` (drop+create) or `append` (preserve collection). Inherits from settings |
| `dedup_columns` | list | — | Columns used for `mkpipe_id` hash deduplication |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |
