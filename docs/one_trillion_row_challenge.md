# [GizmoSQL](https://gizmodata.com/gizmosql) One-Trillion Row Challenge results

GizmoSQL completed the 1 trillion row challenge!  GizmoSQL is powered by [DuckDB](https://duckdb.org) and [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)

We launched a `r8gd.metal-48xl` EC/2 instance (costing $14.1082 on-demand, and $2.8216 spot) in region: `us-east-1` using script: `launch_aws_instance.sh` in the attached zip file.  We have an S3 end-point in the VPC to avoid egress costs.

That script calls script: `scripts/mount_nvme_aws.sh` which creates a RAID 0 storage array from the local NVMe disks - creating a single volume that has: 11.4TB in storage.

We launched the GizmoSQL Docker container using `scripts/run_gizmosql_aws.sh` - which includes the AWS S3 CLI utilities (so we can copy data, etc.).

We then copied the S3 data from `s3://coiled-datasets-rp/1trc/` to the local NVMe RAID 0 array volume - using attached script: `scripts/copy_coiled_data_from_s3.sh` - and it used: 2.3TB of the storage space.  This copy step took: 11m23.702s (costing $2.78 on-demand, and $0.54 spot).

We then launched GizmoSQL via the steps after the docker stuff in: `scripts/run_gizmosql_aws.sh` - and connected remotely from our laptop via the GizmoSQL JDBC Driver - (see repo: https://github.com/gizmodata/gizmosql for details) - and ran this SQL to create a view on top of the parquet datasets:

```
CREATE VIEW measurements_1trc
AS
SELECT *
  FROM read_parquet('data/coiled-datasets-rp/1trc/*.parquet');
```

Row count:

<img width="719" alt="Image" src="https://github.com/user-attachments/assets/d0e904eb-4f30-4eed-afd2-02c593b49a98" />

We then ran the test query:
```
SELECT station, min(measure), max(measure), avg(measure)
FROM measurements_1trc
GROUP BY station
ORDER BY station;
```

<img width="895" alt="Image" src="https://github.com/user-attachments/assets/d93e21b9-3b0c-478a-88eb-50bfa43b556e" />

It took: 0:02:22 (142s) the first execution (cold-start) - at an EC/2 on-demand cost of: $0.56, and a spot cost of: $0.11

It took: 0:02:09 (129s) the second execution (warm-start) - at an EC/2 on-demand cost of: $0.51, and a spot cost of: $0.10

Here are the query results:
[1trc_results.csv](https://github.com/user-attachments/files/21023289/1trc_results.csv)

Scripts:
[1trc_gizmosql.zip](https://github.com/user-attachments/files/21019516/1trc_gizmosql.zip)

Side note:
Query: `SELECT COUNT(*) FROM measurements_1trc;` takes: 21.8s
