
This directory has a set of SQL procedures to create/refresh a continuous aggregates from tiered data on S3.

IMPORTANT NOTE:
This procedure only adds the **tiered portion** of the hypertable's data to the continuous aggregate.

We use a producer-consumer pattern to incrementally build the continuous aggregate from tiered data. We first identify the time range that we will refresh, split it into a bunch of smaller intervals and then start refreshing these intervals.

Producer: The procedure _timescaledb_additional.schedule_osm_cagg_refresh_ finds the time range of the tiered data for the hypertable (corresponding to the continuous aggregate) and splits the range into a set of time intervals. These intervals are added to the _timescaledb_additional.incremental_continuous_aggregate_refreshes_ table.

Consumer: The procedure '_timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner_ picks the time intervals from the incremental_continuous_aggregate_refreshes table and calls refresh_continuous_aggregate on  these intervals until the list is exhausted.

The continuous_aggregate_refreshes table can be used to monitor progress of the refresh.

How do I run this?
1. CALL _timescaledb_additional.schedule_osm_cagg_refresh('<name of continuous aggregate>')_;
2. Add a job to call  _timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner_
SELECT
    add_job(
        '_timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner',
        '1 minute',
        config => '{"enable_tiered_reads": true}'
    )

enable_tiered_reads = true config is necessary if your default  DB settings for timescaleb_osm.enable_tiered_reads GUC is false.
3. Check refresh progress by querying the osm_incremental_refresh_status view. 
SELECT * FROM _timescaledb_additional.osm_incremental_refresh_status_

IMPORTANT NOTE:
A cagg will be refreshed by exactly 1 job using this framework even if you choose to add multiple jobs to run the task_refresh_continuous_aggregate_incremental_runner procedure.
