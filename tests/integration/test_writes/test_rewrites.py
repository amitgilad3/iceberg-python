import uuid
from datetime import datetime, date, timezone

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession
import pyarrow.parquet as pq

from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table.locations import load_location_provider
from pyiceberg.types import NestedField, StringType, DateType
from utils import _create_table
from pyiceberg.io.pyarrow import _check_pyarrow_schema_compatible, _dataframe_to_data_files


@pytest.mark.integration
def test_data_files_amit(spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    # Step 1: Create a table
    identifier = "default.test_table_rewrite"
    TABLE_SCHEMA = Schema(
        NestedField(field_id=1, name="name", field_type=StringType(), required=False),
        NestedField(field_id=2, name="date", field_type=DateType(), required=False),
        NestedField(field_id=3, name="country", field_type=StringType(), required=False),
    )
    data = {
        "name": ["Alice", "Bob", "Charlie"],
        "date": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
        "country": ["USA", "Canada", "UK"]
    }
    data1 = {
        "name": ["AMIT", "AMIT", "AMIT"],
        "date": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
        "country": ["USA", "Canada", "UK"]
    }
    combined = {
        "name": ["Alice", "Bob", "Charlie","AMIT", "AMIT", "AMIT"],
        "date": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3),date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
        "country": ["USA", "Canada", "UK","USA", "Canada", "UK"]
    }
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [], schema=TABLE_SCHEMA)

    arrow_table = pa.Table.from_pydict(data)
    tbl.append(arrow_table)
    arrow_table = pa.Table.from_pydict(data1)
    tbl.append(arrow_table)

    df = pa.Table.from_pydict(combined).sort_by("name")

    data_files = _dataframe_to_data_files(table_metadata=tbl.metadata, df=df, io=tbl.io)
    # Step 2: Append some data to the table



    # Step 3: Rewrite the table by adding and deleting files
    files = [f.file for f in tbl.scan().plan_files()]

    spark.sql(  f"""
        SELECT *
        FROM {identifier}.files
     """).show(10,False)

    tbl.rewrite(added_files=data_files, deleted_files=files)

    # Step 4: Verify the results
    # con = tbl.scan().to_duckdb(table_name="test_rewrite")
    # result = con.execute("SELECT count(1) from test_rewrite").fetchall()[0][0]
    # assert result == 6, f"Expected 2 rows, but got {result}"
    spark.sql(  f"""
        SELECT *
        FROM {identifier}.files
     """).show(10,False)
    spark.sql(  f"""
        SELECT *
        FROM {identifier}.snapshots
     """).show(10,False)

    spark.sql(  f"""
        SELECT *
        FROM {identifier}
     """).show(10,False)
    # rows = spark.sql(
    #     f"""
    #     SELECT added_data_files_count, existing_data_files_count, deleted_data_files_count
    #     FROM {identifier}.all_manifests
    # """
    # ).collect()


