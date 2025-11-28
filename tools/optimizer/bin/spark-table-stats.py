#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
import os
from pyspark.sql import SparkSession

TABLE_STATS_SQL_TEMPLATE = """
SELECT
  COUNT(*) AS file_count,
  SUM(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files,
  SUM(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_files,
  SUM(CASE WHEN content = 2 THEN 1 ELSE 0 END) AS equality_delete_files,
  SUM(CASE WHEN file_size_in_bytes < 100000 THEN 1 ELSE 0 END) AS small_files,
  AVG(POWER(100000 - LEAST(100000, file_size_in_bytes), 2)) AS data_size_mse,
  AVG(file_size_in_bytes) AS avg_size,
  SUM(file_size_in_bytes) AS total_size
FROM %s.files
"""


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--table", required=True, help="catalog.schema.table")
  parser.add_argument("--output", required=True, help="JSON-lines output path")
  args = parser.parse_args()

  spark = SparkSession.builder.appName("table-stats").getOrCreate()
  sql = TABLE_STATS_SQL_TEMPLATE % args.table
  row = spark.sql(sql).first()

  stats = {"identifier": args.table, "stats-type": "table"}
  for field in row.__fields__:
    val = row[field]
    if val is None:
      continue
    # Cast Spark numeric types to plain Python primitives for JSON
    if isinstance(val, float):
      stats[field] = float(val)
    elif isinstance(val, int):
      stats[field] = int(val)
    else:
      stats[field] = val

  os.makedirs(os.path.dirname(args.output), exist_ok=True)
  with open(args.output, "w", encoding="utf-8") as fh:
    fh.write(json.dumps(stats) + "\n")

  spark.stop()


if __name__ == "__main__":
  main()
