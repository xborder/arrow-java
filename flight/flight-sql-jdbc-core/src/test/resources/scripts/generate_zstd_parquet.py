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

"""
Generate a ZSTD-compressed Parquet file for testing the Flight JDBC driver.

This script creates a Parquet file with ZSTD compression containing test data
with the following schema:
- id: int64 (non-nullable)
- name: string (nullable)
- value: int32 (nullable)
- score: float64 (nullable)
- binary_data: binary (nullable)

Usage:
    python generate_zstd_parquet.py <output_path> [row_count]

Requirements:
    pip install pyarrow pandas
"""

import sys
import pyarrow as pa
import pyarrow.parquet as pq


def generate_test_data(row_count: int = 100):
    """Generate test data matching the expected schema."""
    ids = [1000 + i for i in range(row_count)]
    names = [f"Record_{i}" for i in range(row_count)]
    values = [i * 10 for i in range(row_count)]
    scores = [i * 1.5 for i in range(row_count)]
    binary_data = [f"binary_{i}".encode() for i in range(row_count)]

    return {
        "id": ids,
        "name": names,
        "value": values,
        "score": scores,
        "binary_data": binary_data,
    }


def create_arrow_table(data: dict) -> pa.Table:
    """Create an Arrow table with the specified schema."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.utf8(), nullable=True),
            pa.field("value", pa.int32(), nullable=True),
            pa.field("score", pa.float64(), nullable=True),
            pa.field("binary_data", pa.binary(), nullable=True),
        ]
    )

    arrays = [
        pa.array(data["id"], type=pa.int64()),
        pa.array(data["name"], type=pa.utf8()),
        pa.array(data["value"], type=pa.int32()),
        pa.array(data["score"], type=pa.float64()),
        pa.array(data["binary_data"], type=pa.binary()),
    ]

    return pa.Table.from_arrays(arrays, schema=schema)


def write_zstd_parquet(table: pa.Table, output_path: str):
    """Write the table to a Parquet file with ZSTD compression."""
    pq.write_table(
        table,
        output_path,
        compression="zstd",
        compression_level=3,  # Default ZSTD compression level
    )
    print(f"Written ZSTD-compressed Parquet file to: {output_path}")
    print(f"  Rows: {table.num_rows}")
    print(f"  Columns: {table.num_columns}")
    print(f"  Schema: {table.schema}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_zstd_parquet.py <output_path> [row_count]")
        sys.exit(1)

    output_path = sys.argv[1]
    row_count = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    data = generate_test_data(row_count)
    table = create_arrow_table(data)
    write_zstd_parquet(table, output_path)


if __name__ == "__main__":
    main()

