# Create 4 (four) custom jobs
```sql
SELECT
    add_job(
        '_timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner',
        '1 minute',
        config => '{"enable_tiered_reads": true}'
    )
FROM
    generate_series(1, 4);
```

# Produce ranges to be refreshed
```sql
CALL _timescaledb_additional.schedule_osm_cagg_refresh('g_data_16_41_1minute'); -- An specific CAgg
CALL _timescaledb_additional.schedule_osm_cagg_refresh(); -- All CAggs
```

# Check the queue status
```sql
SELECT * FROM _timescaledb_additional.osm_incremental_refresh_status \watch
```

# Check the jobs execution
```sql
SELECT * FROM _timescaledb_additional.job_cagg_refresh_status \watch
```
