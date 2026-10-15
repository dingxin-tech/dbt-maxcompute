# Replace a non-partitioned table without recreating it

Use `truncate_insert` when every run should replace a non-partitioned table's
data while retaining the existing table object. This is an opt-in incremental
strategy; existing strategies and defaults are unchanged.

Existing project or package overrides of `get_incremental_truncate_insert_sql`
or its dispatched implementation keep their own behavior. The built-in checks
below apply only when both macros resolve to the adapter's implementation.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='truncate_insert',
    full_refresh=false,
    on_schema_change='fail'
) }}

select order_id, amount
from {{ ref('all_orders') }}
```

Return the **complete desired contents** of the target on every run. Do not add
an `is_incremental()` watermark filter: rows absent from the result are removed.
An empty result clears all old rows. `unique_key` is not required and does not
merge or deduplicate rows; the target table's own key semantics still apply.

The first run creates the target using the existing incremental materialization.
Later runs build the model result in a temporary table, then execute:

```sql
insert overwrite table target_table
select order_id, amount
from temporary_table
```

The adapter projects columns in the target table's actual order, even if the
model query reorders them, and cleans up the temporary table after success.
There is no separate `TRUNCATE`, target `DROP`, or target rename. A failed
temporary-table build therefore leaves the target's old rows intact. Do not
run concurrent writers against the same target.

SQL models and MaxFrame Python models use the same final overwrite. Python
models must also return the complete snapshot and must not filter on
`dbt.is_incremental`.

## Supported configuration

- Only non-partitioned targets are supported. `partition_by`, nonempty
  `partitions`, and targets that are already partitioned are rejected. Continue
  using `insert_overwrite` for partitioned models.
- `predicates` and `incremental_predicates` are rejected: this strategy always
  replaces the whole table.
- An existing view or external relation is rejected instead of replaced.
- For this strategy, rebuilding an existing target with full-refresh is
  rejected before hooks or target changes. Set `full_refresh=false`, as in the
  example, to preserve the table even when a project run uses `--full-refresh`.
  Full-refresh behavior for every existing strategy remains unchanged.
- Existing `on_schema_change`, column expansion, `grants`, `persist_docs`, and
  hooks retain their usual behavior. Use `on_schema_change='fail'` when the
  target schema must stay fixed. Explicit schema changes, grants, and hooks can
  still change metadata or permissions.

Retaining the table avoids deleting object-bound Label grants during data
replacement. This strategy does not restore grants already lost to an earlier
drop/create, or change MaxCompute's permission inheritance rules. Validate
table/column labels, grants, and cross-project reads in your environment before
rollout. See MaxCompute's [Label documentation](https://help.aliyun.com/zh/maxcompute/all-statements-set-label)
and [INSERT documentation](https://help.aliyun.com/zh/maxcompute/insert-or-update-data-into-a-table-or-a-static-partition).
