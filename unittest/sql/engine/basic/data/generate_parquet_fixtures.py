#!/usr/bin/env python3
# Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0.
"""Reproduce small Parquet fixtures with the independent Apache Arrow writer.

Only fixture generation needs pyarrow. Integration tests use the checked-in
files and execute SQL solely with seekdb.
"""
from datetime import date, datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


def generate(directory):
    data = pa.table({
        "b": pa.array([True, None, False, True], type=pa.bool_()),
        "i32": pa.array([1, None, -3, 4], type=pa.int32()),
        "i64": pa.array([9223372036854775807, None, -9223372036854775808, 10], type=pa.int64()),
        "f32": pa.array([1.25, None, 3.5, -4], type=pa.float32()),
        "f64": pa.array([1.5, None, 2.5, -4.5], type=pa.float64()),
        "text": pa.array(["你好", None, 'quote"\nnext', ""], type=pa.string()),
        "day": pa.array([date(2024, 2, 29), None, date(1970, 1, 1), date(2026, 10, 27)], type=pa.date32()),
        "created": pa.array([datetime(2024, 2, 29, 23, 59, 59, 123456), None,
                             datetime(1970, 1, 1), datetime(2026, 10, 27, 1, 2, 3)], type=pa.timestamp("us")),
    })
    for version in ("1.0", "2.0"):
        for encoded in (False, True):
            name = "arrow_v" + version[0] + ("_dictionary_snappy" if encoded else "_plain") + ".parquet"
            pq.write_table(data, directory / name, row_group_size=2,
                           compression="snappy" if encoded else "NONE", use_dictionary=encoded,
                           data_page_version=version, version="2.6")


if __name__ == "__main__":
    generate(Path(__file__).resolve().parent)
