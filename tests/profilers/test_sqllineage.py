import os
import sys
import unittest

from sqlglot.errors import ParseError

sys.path.insert(0, os.path.abspath("../.."))

from quollio_core.helper.core import new_global_id
from quollio_core.profilers.lineage import LineageInput, LineageInputs
from quollio_core.profilers.sqllineage import SQLLineage, Table


class TestSQLLineage(unittest.TestCase):
    def setUp(self):
        self.client = SQLLineage()

    def test_gen_table_level_lineage(self):
        # TODO: Add SQL case using CTE.
        test_cases = [
            # Normal queries
            {
                "desc": "SQL which doesn't contain both dest and src schema and db",
                "input": {
                    "sql": """
                        create table dest_table as
                        select *
                        from
                          test1 a
                        inner join
                          test2 b
                        on a.id = b.id
                        """,
                    "dialect": "",
                    "src_db": "dwh",
                    "src_db_schema": "public",
                    "dest_db": "mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="mart", db_schema="public", table="dest_table"),
                    "src": {
                        Table(db="dwh", db_schema="public", table="test1"),
                        Table(db="dwh", db_schema="public", table="test2"),
                    },
                },
            },
            {
                "desc": "Test dest_db and dest_schema prioritize database and schema written in sql.",
                "input": {
                    "sql": """
                        create table mart1.public1.dest_table as
                        select *
                        from
                          test1 a
                        inner join
                          test2 b
                        on a.id = b.id
                        """,
                    "dialect": "",
                    "src_db": "dwh",
                    "src_db_schema": "public",
                    "dest_db": "mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="mart1", db_schema="public1", table="dest_table"),
                    "src": {
                        Table(db="dwh", db_schema="public", table="test1"),
                        Table(db="dwh", db_schema="public", table="test2"),
                    },
                },
            },
            {
                "desc": "Test src_db and src_schema prioritize database and schema written in sql.",
                "input": {
                    "sql": """
                        create table mart1.public1.dest_table as
                        select *
                        from
                          dwh1.public1.test1 a
                        inner join
                          dwh1.public1.test2 b
                        on a.id = b.id
                        """,
                    "dialect": "",
                    "src_db": "dwh",
                    "src_db_schema": "public",
                    "dest_db": "mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="mart1", db_schema="public1", table="dest_table"),
                    "src": {
                        Table(db="dwh1", db_schema="public1", table="test1"),
                        Table(db="dwh1", db_schema="public1", table="test2"),
                    },
                },
            },
            {
                "desc": "Test the database and schema of test1 are the ones specified in src.",
                "input": {
                    "sql": """
                        create table mart1.public1.dest_table as
                        select *
                        from
                          test1 a
                        inner join
                          dwh1.public1.test2 b
                        on a.id = b.id
                        """,
                    "dialect": "",
                    "src_db": "dwh",
                    "src_db_schema": "public",
                    "dest_db": "mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="mart1", db_schema="public1", table="dest_table"),
                    "src": {
                        Table(db="dwh", db_schema="public", table="test1"),
                        Table(db="dwh1", db_schema="public1", table="test2"),
                    },
                },
            },
            {
                "desc": "Test the database and schema of dest_table are the ones specified in dest.",
                "input": {
                    "sql": """
                        create table dest_table as
                        select *
                        from
                          test1 a
                        inner join
                          dwh1.public1.test2 b
                        on a.id = b.id
                        """,
                    "dialect": "",
                    "src_db": "dwh",
                    "src_db_schema": "public",
                    "dest_db": "mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="mart", db_schema="public", table="dest_table"),
                    "src": {
                        Table(db="dwh", db_schema="public", table="test1"),
                        Table(db="dwh1", db_schema="public1", table="test2"),
                    },
                },
            },
            {
                "desc": "Test the database and schema of dest_table are specified in neither input nor sql.",
                "input": {
                    "sql": """
                        create table dest_table as
                        select *
                        from
                          test1 a
                        inner join
                          test2 b
                        on a.id = b.id
                        """,
                    "dialect": "",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="", db_schema="", table="dest_table"),
                    "src": {
                        Table(db="", db_schema="", table="test1"),
                        Table(db="", db_schema="", table="test2"),
                    },
                },
            },
            # Snowflake
            {
                "desc": "simple ctas",
                "input": {
                    "sql": """
                        create table mart.public.dest_table as
                        select *
                        from db_dwh.public.test1
                        """,
                    "dialect": "snowflake",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="MART", db_schema="PUBLIC", table="DEST_TABLE"),
                    "src": {Table(db="DB_DWH", db_schema="PUBLIC", table="TEST1")},
                },
            },
            {
                "desc": "ctas using two source tables",
                "input": {
                    "sql": """
                        create table mart.public.dest_table as
                        select *
                        from dwh.public.test1 a
                        inner join
                        dwh.public.test2 b
                        on a.id = b.id
                        """,
                    "dialect": "snowflake",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="MART", db_schema="PUBLIC", table="DEST_TABLE"),
                    "src": {
                        Table(db="DWH", db_schema="PUBLIC", table="TEST1"),
                        Table(db="DWH", db_schema="PUBLIC", table="TEST2"),
                    },
                },
            },
            {
                "desc": "ctas not having dest db. DB is expected to empty string.",
                "input": {
                    "sql": """
                        create table public.dest_table as
                        select *
                        from dwh.public.test1 a
                        inner join
                        dwh.public.test2 b
                        on a.id = b.id
                        """,
                    "dialect": "snowflake",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="", db_schema="PUBLIC", table="DEST_TABLE"),
                    "src": {
                        Table(db="DWH", db_schema="PUBLIC", table="TEST1"),
                        Table(db="DWH", db_schema="PUBLIC", table="TEST2"),
                    },
                },
            },
            {
                "desc": "ctas having dest db. dest_db is expected to be assigned.",
                "input": {
                    "sql": """
                        create table public.dest_table as
                        select *
                        from dwh.public.test1 a
                        inner join
                        dwh.public.test2 b
                        on a.id = b.id
                        """,
                    "dialect": "snowflake",
                    "dest_db": "db_mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="DB_MART", db_schema="PUBLIC", table="DEST_TABLE"),
                    "src": {
                        Table(db="DWH", db_schema="PUBLIC", table="TEST1"),
                        Table(db="DWH", db_schema="PUBLIC", table="TEST2"),
                    },
                },
            },
            {
                "desc": "ctas not having dest db and schema. DB and schema is expected to empty string.",
                "input": {
                    "sql": """
                        create table dest_table as
                        select *
                        from dwh.public.test1 a
                        inner join
                        dwh.public.test2 b
                        on a.id = b.id
                        """,
                    "dialect": "snowflake",
                    "db": "db_mart",
                    "db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="", db_schema="", table="DEST_TABLE"),
                    "src": {
                        Table(db="DWH", db_schema="PUBLIC", table="TEST1"),
                        Table(db="DWH", db_schema="PUBLIC", table="TEST2"),
                    },
                },
            },
            {
                "desc": "ctas not having src database.",
                "input": {
                    "sql": """
                        create table mart.public.dest_table as
                        select *
                        from public.test1 a
                        inner join
                        db_dwh.public.test2 b
                        on a.id = b.id
                        """,
                    "dialect": "snowflake",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="MART", db_schema="PUBLIC", table="DEST_TABLE"),
                    "src": {
                        Table(db="", db_schema="PUBLIC", table="TEST1"),
                        Table(db="DB_DWH", db_schema="PUBLIC", table="TEST2"),
                    },
                },
            },
            {
                "desc": "ctas not having src db and schema.",
                "input": {
                    "sql": """
                        create table mart.public.dest_table as
                        select *
                        from test1 a
                        inner join
                        dwh.public.test2 b
                        on a.id = b.id
                        """,
                    "dialect": "snowflake",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="MART", db_schema="PUBLIC", table="DEST_TABLE"),
                    "src": {
                        Table(db="", db_schema="", table="TEST1"),
                        Table(db="DWH", db_schema="PUBLIC", table="TEST2"),
                    },
                },
            },
            {
                "desc": "ctas not having src database. db is used as a source database.",
                "input": {
                    "sql": """
                        create table mart.public.dest_table as
                        select *
                        from public.test1 a
                        inner join
                        db_dwh.public.test2 b
                        on a.id = b.id
                        """,
                    "dialect": "snowflake",
                    "src_db": "dwh",  # MEMO: Use this value as a database and a schema of source table.
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="MART", db_schema="PUBLIC", table="DEST_TABLE"),
                    "src": {
                        Table(db="DWH", db_schema="PUBLIC", table="TEST1"),
                        Table(db="DB_DWH", db_schema="PUBLIC", table="TEST2"),
                    },
                },
            },
            {
                "desc": "ctas not having src db and schema. db and db_schema are used as a source db and schema.",
                "input": {
                    "sql": """
                        create table mart.public.dest_table as
                        select *
                        from test1 a
                        inner join
                        dwh.public.test2 b
                        on a.id = b.id
                        """,
                    "dialect": "snowflake",
                    "src_db": "dwh",
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="MART", db_schema="PUBLIC", table="DEST_TABLE"),
                    "src": {
                        Table(db="DWH", db_schema="PUBLIC", table="TEST1"),
                        Table(db="DWH", db_schema="PUBLIC", table="TEST2"),
                    },
                },
            },
            {
                "desc": "simple merge stmt",
                "input": {
                    "sql": """
                        MERGE INTO mart.public.target_table t
                        USING mart.public.test1 s
                        ON t.id = s.id
                        WHEN MATCHED THEN
                          UPDATE SET t.delete_flag = 0
                        WHEN NOT MATCHED THEN
                          INSERT
                            (id, name, delete_flag)
                          VALUES
                            (s.id, s.name, s.delete_flag)
                        """,
                    "dialect": "snowflake",
                    "db": "db_dwh",
                    "db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="MART", db_schema="PUBLIC", table="TARGET_TABLE"),
                    "src": {
                        Table(db="MART", db_schema="PUBLIC", table="TEST1"),
                    },
                },
            },
            {
                "desc": "simple merge stmt2",
                "input": {
                    "sql": """
                        MERGE INTO mart.public.test1 AS target
                        USING (
                            SELECT
                                ID,
                                CODE,
                                NAME,
                            FROM dwh.public.test2
                        ) AS source
                        ON target.ID = source.ID AND target.CODE = source.CODE
                        WHEN MATCHED THEN
                            UPDATE SET
                                target.ID = source.ID
                                , target.CODE = source.CODE
                                , target.NAME = source.NAME
                        WHEN NOT MATCHED THEN
                            INSERT (
                                ID
                                , CODE
                                , NAME
                            ) VALUES (
                                source.ID
                                , source.CODE
                                , source.NAME
                            )
                        """,
                    "dialect": "snowflake",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="MART", db_schema="PUBLIC", table="TEST1"),
                    "src": {
                        Table(db="DWH", db_schema="PUBLIC", table="TEST2"),
                    },
                },
            },
            {
                "desc": "simple merge stmt3",
                "input": {
                    "sql": """
                        MERGE INTO mart.public.members m
                          USING (
                          SELECT id, date
                          FROM dwh.public.signup
                          WHERE DATEDIFF(day, CURRENT_DATE(), signup.date::DATE) < -30) s ON m.id = s.id
                          WHEN MATCHED THEN UPDATE SET m.fee = 40;
                        """,
                    "dialect": "snowflake",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="MART", db_schema="PUBLIC", table="MEMBERS"),
                    "src": {
                        Table(db="DWH", db_schema="PUBLIC", table="SIGNUP"),
                    },
                },
            },
            {
                "desc": "merge stmt using two source tables",
                "input": {
                    "sql": """
                        MERGE INTO mart.public.members m
                          USING (
                          SELECT id, date
                          FROM dwh.public.signup a inner join dwh.public.signup1 b on a.id = b.id
                          WHERE DATEDIFF(day, CURRENT_DATE(), signup.date::DATE) < -30) s ON m.id = s.id
                          WHEN MATCHED THEN UPDATE SET m.fee = 40;
                        """,
                    "dialect": "snowflake",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="MART", db_schema="PUBLIC", table="MEMBERS"),
                    "src": {
                        Table(db="DWH", db_schema="PUBLIC", table="SIGNUP"),
                        Table(db="DWH", db_schema="PUBLIC", table="SIGNUP1"),
                    },
                },
            },
            # Athena https://docs.aws.amazon.com/ja_jp/athena/latest/ug/ctas-examples.html
            {
                "desc": "Athena ctas query1",
                "input": {
                    "sql": """
                        CREATE TABLE db.mart.new_table AS
                        SELECT *
                        FROM old_table;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="new_table"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="old_table"),
                    },
                },
            },
            {
                "desc": "Athena ctas query2",
                "input": {
                    "sql": """
                        CREATE TABLE db.mart.new_table AS
                        SELECT
                           column_1
                           , column_2
                        FROM
                           db_dwh.public.old_table_1, db_dwh.public.old_table_2;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="new_table"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="old_table_1"),
                        Table(db="db_dwh", db_schema="public", table="old_table_2"),
                    },
                },
            },
            {
                "desc": "Athena ctas query with no data",
                "input": {
                    "sql": """
                        CREATE TABLE db.mart.new_table AS
                        SELECT
                           column_1
                           , column_2
                        FROM
                           db_dwh.public.old_table_1, db_dwh.public.old_table_2;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="new_table"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="old_table_1"),
                        Table(db="db_dwh", db_schema="public", table="old_table_2"),
                    },
                },
            },
            {
                "desc": "Athena ctas query non partitioned table",
                "input": {
                    "sql": """
                        CREATE TABLE db.mart.ctas_csv_unpartitioned
                        WITH (
                             format = 'TEXTFILE',
                             external_location = 's3://my_athena_results/ctas_csv_unpartitioned/')
                        AS SELECT key1, name1, address1, comment1
                        FROM db_dwh.public.table1;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="ctas_csv_unpartitioned"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="table1"),
                    },
                },
            },
            {
                "desc": "Athena ctas query partitioned table",
                "input": {
                    "sql": """
                        CREATE TABLE db.mart.ctas_json_partitioned
                        WITH (
                             format = 'JSON',
                             external_location = 's3://my_athena_results/ctas_json_partitioned/',
                             partitioned_by = ARRAY['key1'])
                        AS select name1, address1, comment1, key1
                        FROM db_dwh.public.table1;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="ctas_json_partitioned"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="table1"),
                    },
                },
            },
            {
                "desc": "Athena ctas query partitioned table",
                "input": {
                    "sql": """
                        CREATE TABLE db.mart.ctas_json_partitioned
                        WITH (
                             format = 'JSON',
                             external_location = 's3://my_athena_results/ctas_json_partitioned/',
                             partitioned_by = ARRAY['key1'])
                        AS select name1, address1, comment1, key1
                        FROM db_dwh.public.table1;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="ctas_json_partitioned"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="table1"),
                    },
                },
            },
            {
                "desc": "Athena ctas query partitioned table",
                "input": {
                    "sql": """
                        CREATE TABLE db.mart.ctas_json_partitioned
                        WITH (
                             format = 'JSON',
                             external_location = 's3://my_athena_results/ctas_json_partitioned/',
                             partitioned_by = ARRAY['key1'])
                        AS select t1.name1, t1.address1, t2.comment1, t2.key1
                        FROM db_dwh.public.table1 t1
                        INNER JOIN db_dwh.public.table2 t2
                        ON t1.name = t2.name
                        ;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="ctas_json_partitioned"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="table1"),
                        Table(db="db_dwh", db_schema="public", table="table2"),
                    },
                },
            },
            {
                "desc": "Athena view ddl",
                "input": {
                    "sql": """
                        CREATE VIEW orders_by_date AS
                        SELECT orderdate, sum(totalprice) AS price
                        FROM orders
                        GROUP BY orderdate;
                        ;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "dest_db": "db_mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db_mart", db_schema="public", table="orders_by_date"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="orders"),
                    },
                },
            },
            {
                "desc": "Athena create or replace view ddl",
                "input": {
                    "sql": """
                        CREATE OR REPLACE VIEW test AS
                        SELECT orderkey, orderstatus, totalprice / 4 AS quarter
                        FROM orders;
                        ;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "dest_db": "db_mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db_mart", db_schema="public", table="test"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="orders"),
                    },
                },
            },
            {
                "desc": "Athena merge statement",
                "input": {
                    "sql": """
                        MERGE INTO iceberg_table_sample as ice1
                        USING iceberg2_table_sample as ice2
                        ON ice1.col1 = ice2.col1
                        WHEN NOT MATCHED
                        THEN INSERT (col1)
                              VALUES (ice2.col1)
                        ;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "dest_db": "db_mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db_mart", db_schema="public", table="iceberg_table_sample"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="iceberg2_table_sample"),
                    },
                },
            },
            {
                "desc": "Athena select insert",
                "input": {
                    "sql": """
                        insert into iceberg_table_sample
                        select *
                        from iceberg2_table_sample
                        where id = 1
                        ;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "dest_db": "db_mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db_mart", db_schema="public", table="iceberg_table_sample"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="iceberg2_table_sample"),
                    },
                },
            },
            {
                "desc": "Athena update select",
                "input": {
                    "sql": """
                        update iceberg_table_sample
                        set id = 1
                        where name in (select name from iceberg2_table_sample)
                        ;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "dest_db": "db_mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db_mart", db_schema="public", table="iceberg_table_sample"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="iceberg2_table_sample"),
                    },
                },
            },
            # Oracle. Refer to the following docs.
            # https://www.dba-oracle.com/t_create_table_select_ctas.htm
            # https://www.orafaq.com/wiki/CTAS.
            {
                "desc": "Oracle ctas query",
                "input": {
                    "sql": """
                        create table db.mart.test1
                        as
                        select * from db.dwh.src_table
                        order by primary_index_key_values;
                        """,
                    "dialect": "oracle",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "expect_succeed": True,
                },
                # MEMO: Confirm whether the objects of oracle db is always upper case or not.
                "expect": {
                    "dest": Table(db="DB", db_schema="MART", table="TEST1"),
                    "src": {
                        Table(db="DB", db_schema="DWH", table="SRC_TABLE"),
                    },
                },
            },
            {
                "desc": "Oracle ctas query specifying tablespace",
                "input": {
                    "sql": """
                        create table db.mart.test1 tablespace tblspace as
                        select
                          *
                        from
                          db.dwh.src_table
                        order by
                          primary_index_key_values;
                        """,
                    "dialect": "oracle",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "expect_succeed": False,
                },
                # MEMO: Parse failed due to the tablespace obj.
                "expect": {
                    "dest": Table(db="DB", db_schema="MART", table="TEST1"),
                    "src": {
                        Table(db="DB", db_schema="DWH", table="SRC_TABLE"),
                    },
                },
            },
            {
                "desc": "Oracle ctas query specifying nologging",
                "input": {
                    "sql": """
                        CREATE TABLE db.mart.emp4 NOLOGGING PARALLEL 4 AS
                        SELECT
                            *
                        FROM
                            db.dwh.emp;
                        """,
                    "dialect": "oracle",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "expect_succeed": False,
                },
                # MEMO: Parse failed due to the tablespace obj.
                "expect": {
                    "dest": Table(db="DB", db_schema="MART", table="EMP4"),
                    "src": {
                        Table(db="DB", db_schema="DWH", table="EMP"),
                    },
                },
            },
            # https://docs.oracle.com/cd/E16338_01/server.112/b56299/statements_8004.htm
            {
                "desc": "Oracle normal view ddl",
                "input": {
                    "sql": """
                        CREATE VIEW emp_view AS
                        SELECT last_name, salary*12 annual_salary
                        FROM employees
                        WHERE department_id = 20;
                        """,
                    "dialect": "oracle",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="DB", db_schema="MART", table="EMP_VIEW"),
                    "src": {
                        Table(db="DB", db_schema="DWH", table="EMPLOYEES"),
                    },
                },
            },
            {
                "desc": "Oracle restriction view ddl",
                "input": {
                    "sql": """
                        CREATE VIEW emp_sal (emp_id, last_name,
                              email UNIQUE RELY DISABLE NOVALIDATE,
                           CONSTRAINT id_pk PRIMARY KEY (emp_id) RELY DISABLE NOVALIDATE)
                           AS SELECT employee_id, last_name, email FROM employees;
                        """,
                    "dialect": "oracle",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": False,
                },
                "expect": {
                    "dest": Table(db="DB", db_schema="MART", table="EMP_SAL"),
                    "src": {
                        Table(db="DB", db_schema="DWH", table="EMPLOYEES"),
                    },
                },
            },
            {
                "desc": "Oracle readonly view ddl",
                "input": {
                    "sql": """
                        CREATE VIEW customer_ro (name, language, credit)
                              AS SELECT cust_last_name, nls_language, credit_limit
                              FROM customers
                              WITH READ ONLY;
                        """,
                    "dialect": "oracle",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": False,
                },
                "expect": {
                    "dest": Table(db="DB", db_schema="MART", table="CUSTOMER_RO"),
                    "src": {
                        Table(db="DB", db_schema="DWH", table="CUSTOMERS"),
                    },
                },
            },
            # https://docs.oracle.com/cd/F19136_01/sqlrf/MERGE.html#GUID-5692CCB7-24D9-4C0E-81A7-A22436DC968F
            {
                "desc": "Oracle merge statement",
                "input": {
                    "sql": """
                        MERGE INTO bonuses D
                           USING (SELECT employee_id, salary, department_id FROM hr.employees
                           WHERE department_id = 80) S
                           ON (D.employee_id = S.employee_id)
                           WHEN MATCHED THEN UPDATE SET D.bonus = D.bonus + S.salary*.01
                             DELETE WHERE (S.salary > 8000)
                           WHEN NOT MATCHED THEN INSERT (D.employee_id, D.bonus)
                             VALUES (S.employee_id, S.salary*.01)
                             WHERE (S.salary <= 8000);
                        """,
                    "dialect": "oracle",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": False,
                },
                "expect": {
                    "dest": Table(db="DB", db_schema="MART", table="BONUSES"),
                    "src": {
                        Table(db="DB", db_schema="HR", table="EMPLOYEES"),
                    },
                },
            },
            {
                "desc": "Oracle select insert",
                "input": {
                    "sql": """
                        INSERT INTO Purchasing.Parts
                        SELECT PartNumber, DeliveryDays
                        FROM Purchasing.SupplyPrice
                        WHERE DeliveryDays < 20;
                        """,
                    "dialect": "oracle",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="DB", db_schema="PURCHASING", table="PARTS"),
                    "src": {
                        Table(db="DB", db_schema="PURCHASING", table="SUPPLYPRICE"),
                    },
                },
            },
            {
                "desc": "Oracle select update",
                "input": {
                    "sql": """
                        update emp
                        set (id,name) = (select id, name from src_emp where name = "a")
                        where id = 1
                        """,
                    "dialect": "oracle",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="DB", db_schema="MART", table="EMP"),
                    "src": {
                        Table(db="DB", db_schema="DWH", table="SRC_EMP"),
                    },
                },
            },
            # Redshift: https://docs.aws.amazon.com/redshift/latest/dg/r_CTAS_examples.html
            {
                "desc": "Redshift ctas query which defines sortkey and distkey",
                "input": {
                    "sql": """
                        create table db.mart.eventdistsort1
                        distkey (eventid)
                        sortkey (eventid, dateid)
                        as
                        select eventid, venueid, dateid, eventname
                        from db.dwh.event;
                        """,
                    "dialect": "redshift",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="eventdistsort1"),
                    "src": {
                        Table(db="db", db_schema="dwh", table="event"),
                    },
                },
            },
            {
                "desc": "Redshift ctas query which defines evenkey",
                "input": {
                    "sql": """
                        create table db.mart.eventdistsort1 diststyle even
                        as
                        select eventid, venueid, dateid, eventname
                        from db.dwh.event;
                        """,
                    "dialect": "redshift",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="eventdistsort1"),
                    "src": {
                        Table(db="db", db_schema="dwh", table="event"),
                    },
                },
            },
            {
                "desc": "Redshift ctas query which defines evenkey",
                "input": {
                    "sql": """
                        create table db.mart.eventdistsort1 diststyle even sortkey (venueid)
                        as
                        select eventid, venueid, dateid, eventname
                        from event;
                        """,
                    "dialect": "redshift",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="eventdistsort1"),
                    "src": {
                        Table(db="db", db_schema="dwh", table="event"),
                    },
                },
            },
            # docs https://docs.aws.amazon.com/ja_jp/redshift/latest/dg/r_CREATE_VIEW.html#r_CREATE_VIEW-examples.
            {
                "desc": "Redshift view query",
                "input": {
                    "sql": """
                        create view myevent as select eventname from event
                        where eventname = 'LeAnn Rimes';
                        """,
                    "dialect": "redshift",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="myevent"),
                    "src": {
                        Table(db="db", db_schema="dwh", table="event"),
                    },
                },
            },
            {
                "desc": "Redshift create or replace view query",
                "input": {
                    "sql": """
                        create or replace view myuser as select lastname from users;
                        """,
                    "dialect": "redshift",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="myuser"),
                    "src": {
                        Table(db="db", db_schema="dwh", table="users"),
                    },
                },
            },
            {
                "desc": "Redshift create or replace view query with no schema binding",
                "input": {
                    "sql": """
                        create view myevent as select eventname from public.event
                        with no schema binding;
                        """,
                    "dialect": "redshift",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="myevent"),
                    "src": {
                        Table(db="db", db_schema="public", table="event"),
                    },
                },
            },
            # https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html#sub-examples-merge
            {
                "desc": "Redshift merge statement1",
                "input": {
                    "sql": """
                        MERGE INTO db.mart.target USING db.dwh.source ON target.id = source.id
                        WHEN MATCHED THEN UPDATE SET id = source.id, name = source.name
                        WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.name);
                        """,
                    "dialect": "redshift",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="target"),
                    "src": {
                        Table(db="db", db_schema="dwh", table="source"),
                    },
                },
            },
            {
                "desc": "Redshift merge statement2",
                "input": {
                    "sql": """
                        MERGE INTO target USING source ON target.id = source.id REMOVE DUPLICATES;
                        """,
                    "dialect": "redshift",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": False,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="target"),
                    "src": {
                        Table(db="db", db_schema="dwh", table="source"),
                    },
                },
            },
            {
                "desc": "Redshift select insert",
                "input": {
                    "sql": """
                        insert into target (select * from source);
                        """,
                    "dialect": "redshift",
                    "src_db": "db",
                    "src_db_schema": "dwh",
                    "dest_db": "db",
                    "dest_db_schema": "mart",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db", db_schema="mart", table="target"),
                    "src": {
                        Table(db="db", db_schema="dwh", table="source"),
                    },
                },
            },
            {
                "desc": "Redshift update select",
                "input": {
                    "sql": """
                        update target
                        set id = 1
                        where name in (select name from source)
                        ;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "dest_db": "db_mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db_mart", db_schema="public", table="target"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="source"),
                    },
                },
            },
            {
                "desc": "Redshift update select from two tables",
                "input": {
                    "sql": """
                        update target
                        set id = 1
                        where name in (select a.name from source as a inner join source1 as b on a.name = b.name)
                        ;
                        """,
                    "dialect": "presto",
                    "src_db": "db_dwh",
                    "src_db_schema": "public",
                    "dest_db": "db_mart",
                    "dest_db_schema": "public",
                    "expect_succeed": True,
                },
                "expect": {
                    "dest": Table(db="db_mart", db_schema="public", table="target"),
                    "src": {
                        Table(db="db_dwh", db_schema="public", table="source"),
                        Table(db="db_dwh", db_schema="public", table="source1"),
                    },
                },
            },
        ]
        for test_case in test_cases:
            test_input = test_case["input"]
            if test_input["expect_succeed"]:
                src, dest = self.client.get_table_level_lineage_source(
                    sql=test_input.get("sql"),
                    dialect=test_input.get("dialect"),
                    src_db=test_input.get("src_db"),
                    src_schema=test_input.get("src_db_schema"),
                    dest_db=test_input.get("dest_db"),
                    dest_schema=test_input.get("dest_db_schema"),
                )
                self.assertEqual(
                    src,
                    test_case["expect"]["src"],
                    "Case {test_case} failed. Expect is {expect} but got {actual}".format(
                        test_case=test_case["desc"], expect=test_case["expect"]["src"], actual=src
                    ),
                )
                self.assertEqual(
                    dest,
                    test_case["expect"]["dest"],
                    "Case {test_case} failed. Expect is {expect} but got {actual}".format(
                        test_case=test_case["desc"], expect=test_case["expect"]["dest"], actual=dest
                    ),
                )
            else:
                with self.assertRaises(ParseError):
                    src, dest = self.client.get_table_level_lineage_source(
                        sql=test_input.get("sql"),
                        dialect=test_input.get("dialect"),
                        src_db=test_input.get("src_db"),
                        src_schema=test_input.get("src_db_schema"),
                        dest_db=test_input.get("dest_db"),
                        dest_schema=test_input.get("dest_db_schema"),
                    )

    def test_gen_lineage_input(self):
        test_cases = [
            {
                "desc": "simple lineage inputs",
                "input": {
                    "tenant_id": "test-tenant",
                    "endpoint": "test-endpoint",
                    "src_tables": {Table(db="DB_DWH", db_schema="PUBLIC", table="TEST1")},
                    "dest_table": Table(db="MART", db_schema="PUBLIC", table="DEST_TABLE"),
                },
                "expect": LineageInputs(
                    downstream_global_id=new_global_id(
                        tenant_id="test-tenant",
                        cluster_id="test-endpoint",
                        data_id="MARTPUBLICDEST_TABLE",
                        data_type="table",
                    ),
                    downstream_database_name="MART",
                    downstream_schema_name="PUBLIC",
                    downstream_table_name="DEST_TABLE",
                    downstream_column_name="",
                    upstreams=LineageInput(
                        upstream=[
                            new_global_id(
                                tenant_id="test-tenant",
                                cluster_id="test-endpoint",
                                data_id="DB_DWHPUBLICTEST1",
                                data_type="table",
                            ),
                        ]
                    ),
                ),
            },
            {
                "desc": "lineage inputs with multi source tables",
                "input": {
                    "tenant_id": "test-tenant",
                    "endpoint": "test-endpoint",
                    "src_tables": {
                        Table(db="DB_DWH", db_schema="PUBLIC", table="TEST1"),
                        Table(db="DB_DWH", db_schema="PUBLIC", table="TEST2"),
                    },
                    "dest_table": Table(db="MART", db_schema="PUBLIC", table="DEST_TABLE"),
                },
                "expect": LineageInputs(
                    downstream_global_id=new_global_id(
                        tenant_id="test-tenant",
                        cluster_id="test-endpoint",
                        data_id="MARTPUBLICDEST_TABLE",
                        data_type="table",
                    ),
                    downstream_database_name="MART",
                    downstream_schema_name="PUBLIC",
                    downstream_table_name="DEST_TABLE",
                    downstream_column_name="",
                    upstreams=LineageInput(
                        upstream=[
                            new_global_id(
                                tenant_id="test-tenant",
                                cluster_id="test-endpoint",
                                data_id="DB_DWHPUBLICTEST1",
                                data_type="table",
                            ),
                            new_global_id(
                                tenant_id="test-tenant",
                                cluster_id="test-endpoint",
                                data_id="DB_DWHPUBLICTEST2",
                                data_type="table",
                            ),
                        ]
                    ),
                ),
            },
        ]
        for test_case in test_cases:
            test_input = test_case["input"]
            lineage_inputs = self.client.gen_lineage_input(
                tenant_id=test_input["tenant_id"],
                endpoint=test_input["endpoint"],
                src_tables=test_input["src_tables"],
                dest_table=test_input["dest_table"],
            )
            # MEMO: for testing
            lineage_inputs.upstreams.upstream.sort()
            test_case["expect"].upstreams.upstream.sort()
            self.assertEqual(lineage_inputs, test_case["expect"])


if __name__ == "__main__":
    unittest.main()
