#!/usr/bin/env python3
"""
Aggregate table statistics for all tables in a source metrics table.

Outputs JSON lines compatible with FileStatsReader:
- stats-type: "table"
- identifier: "<database>.<table>"
- identity_partition_size_gb: summed value
- filter_count: summed value

Usage:
  spark-submit tools/optimizer/bin/pinterest-table-stats.py \
    --input-table <source_table> \
    --output /tmp/table-stats.json
"""

import argparse
import json
import sys
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aggregate table stats (identity_partition_size_gb, filter_count) for all tables."
    )
    parser.add_argument(
        "--input-table",
        required=True,
        help="Source table in the default Spark catalog containing raw metrics rows.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output JSON path (local filesystem).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    try:
        query = f"""
          SELECT
            concat(database_name, '.', table_name) AS identifier,
            'table' AS `stats-type`,
            SUM(identity_partition_size_gb) AS `custom-identity_partition_size_gb`,
            SUM(filter_count) AS `custom-filter_count`
          FROM {args.input_table}
          GROUP BY database_name, table_name
        """
        result = spark.sql(query)
    except Exception as e:
        sys.stderr.write(f"Failed to query {args.input_table}: {e}\n")
        sys.exit(1)

    rows = result.collect()
    try:
        with open(args.output, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row.asDict()))
                f.write("\n")
    except Exception as e:
        sys.stderr.write(f"Failed to write output {args.output}: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
