#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Read distinct database/table names from a Spark table and register them in Gravitino.

Behavior:
- Ensures schema exists (create if missing).
- Drops table if it already exists, then creates an empty table with `format=iceberg`.

Usage (example):
  spark-submit maintenance/optimizer/bin/register-metastore-tables.py \\
    --input-table rest.query_analytics.metastore_dump \\
    --uri http://localhost:8090 \\
    --metalake test \\
    --catalog dd
"""

import argparse
import sys
from typing import Dict, Iterable

import requests
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(
        description="Register schemas/tables from metastore_dump into Gravitino."
    )
    parser.add_argument(
        "--input-table",
        required=True,
        help="Source Spark table containing database_name and table_name columns.",
    )
    parser.add_argument(
        "--uri",
        default="http://localhost:8090",
        help="Gravitino base URI (without trailing slash).",
    )
    parser.add_argument(
        "--metalake",
        default="test",
        help="Target metalake name in Gravitino.",
    )
    parser.add_argument(
        "--catalog",
        default="dd",
        help="Target catalog name in Gravitino.",
    )
    parser.add_argument(
        "--table-format",
        default="iceberg",
        help="Table format property used when creating tables.",
    )
    parser.add_argument(
        "--purge-drop",
        action="store_true",
        help="Use purge=true when dropping existing tables (default: soft drop).",
    )
    return parser.parse_args()


def api_base(uri: str) -> str:
    return uri.rstrip("/") + "/api"


def schema_url(base: str, metalake: str, catalog: str) -> str:
    return f"{base}/metalakes/{metalake}/catalogs/{catalog}/schemas"


def table_url(base: str, metalake: str, catalog: str, schema: str) -> str:
    return f"{schema_url(base, metalake, catalog)}/{schema}/tables"


def ensure_schema(session: requests.Session, base: str, metalake: str, catalog: str, schema: str):
    payload = {"name": schema, "comment": "", "properties": {}}
    resp = session.post(schema_url(base, metalake, catalog), json=payload)
    if resp.status_code in (200, 201):
        print(f"[schema] created {schema}")
        return
    if resp.status_code == 409:
        print(f"[schema] exists {schema}, skipping create")
        return
    raise RuntimeError(f"Failed to create schema {schema}: {resp.status_code} {resp.text}")


def drop_table_if_exists(
    session: requests.Session,
    base: str,
    metalake: str,
    catalog: str,
    schema: str,
    table: str,
):
    resp = session.delete(
        f"{table_url(base, metalake, catalog, schema)}/{table}",
        params={"purge": False },
    )
    if resp.status_code in (200, 404):
        print(f"[table] drop {schema}.{table} (status {resp.status_code})")
        return
    raise RuntimeError(
        f"Failed to drop table {schema}.{table}: {resp.status_code} {resp.text}"
    )


def create_table(
    session: requests.Session,
    base: str,
    metalake: str,
    catalog: str,
    schema: str,
    table: str,
    comment: str,
    fmt: str,
):
    payload: Dict = {
        "name": table,
        "comment": comment,
        "columns": [],
        "properties": {"format": fmt},
    }
    resp = session.post(table_url(base, metalake, catalog, schema), json=payload)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Failed to create table {schema}.{table}: {resp.status_code} {resp.text}"
        )
    print(f"[table] created {schema}.{table}")


def iter_identifiers(spark, input_table: str) -> Iterable[Dict[str, str]]:
    df = (
        spark.table(input_table)
        .select("database_name", "table_name")
        .where("database_name IS NOT NULL AND table_name IS NOT NULL")
        .distinct()
    )
    return df.toLocalIterator()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    base = api_base(args.uri)

    session = requests.Session()
    session.headers.update(
        {
            "Content-Type": "application/json",
            "Accept": "application/vnd.gravitino.v1+json",
        }
    )

    try:
        for row in iter_identifiers(spark, args.input_table):
            schema = row["database_name"]
            table = row["table_name"]

            ensure_schema(session, base, args.metalake, args.catalog, schema)
            drop_table_if_exists(
                session,
                base,
                args.metalake,
                args.catalog,
                schema,
                table,
            )
            create_table(
                session,
                base,
                args.metalake,
                args.catalog,
                schema,
                table,
                comment="comment",
                fmt="iceberg",
            )
    except Exception as exc:
        sys.stderr.write(f"Failed: {exc}\n")
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
