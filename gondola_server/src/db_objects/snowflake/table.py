from src.db_objects.snowflake.database_object import DatabaseObject
from src.db_objects.snowflake.column import Column
from typing import Dict
from deepdiff import DeepDiff
import json


class Table(DatabaseObject):
    """
    Representation of a database table
    """

    def __init__(self, attributes: Dict):
        self.table_type = attributes['TABLE_TYPE'],
        self.is_transient = attributes['IS_TRANSIENT']
        self.columns = []
        DatabaseObject.__init__(self, attributes=attributes)

    def compare_column_attr(self, other):

        matched_columns = []
        column_alter_scripts = []

        for other_column in other:
            for column in self.columns:
                if other_column.object_name == column.object_name:
                    matched_columns.append(column.object_name)
                    diff = DeepDiff(column.column_attributes,
                                    other_column.column_attributes)
                    if diff:
                        column.attributes_to_alter = diff.get('values_changed')
                        column_alter_scripts.append(
                            column.create_alter_column())

        for column in self.columns:
            if column.object_name not in matched_columns:
                column.drop_flag = True

        return column_alter_scripts

    def create_alter_statement(self, other, inspect_tgt, inspect_src):

        # DDL to create a new interim table with the new structure.
        src_ddl = other.original_ddl.replace(
            self.object_name, self.object_name+'_INT')
        sql_stmt = self.use_schema_sql() + '\n' + src_ddl

        # Now write the sql to populate this new table with the data from old table
        # Any new columns which are defined as not null, are to be handled through NVL with default values

        insert_cols = ""
        select_cols = ""

        # For any column modifications or drops from new
        for idx, (column_name, data_type, is_nullable) in enumerate(inspect_tgt.get_table_alter_ddl(src=other, tgt=self, set_operator='INTERSECT', src_conn=inspect_src.connection, tgt_conn=inspect_tgt.connection)):
            #print(column_name + ', ' + data_type + ',' + is_nullable)
            if idx > 0:
                insert_cols += ', '
                select_cols += ', '

            insert_cols += column_name
            if is_nullable == 'YES':
                select_cols += column_name
            else:
                if data_type in ['NUMBER', 'DECIMAL', 'NUMERIC', 'INT', 'INTEGER',
                                 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT',
                                 'FLOAT', 'FLOAT4', 'FLOAT8',
                                 'DOUBLE', 'DOUBLE PRECISION', 'REAL']:
                    select_cols += f"NVL({column_name},-1) AS {column_name}"
                elif data_type in ['VARCHAR', 'CHAR', 'CHARACTER', 'STRING', 'TEXT',
                                   'BINARY', 'VARBINARY']:
                    select_cols += f"NVL({column_name},'#') AS {column_name}"
                elif data_type == 'BOOLEAN':
                    select_cols += f"NVL({column_name},false) AS {column_name}"
                elif data_type == 'DATE':
                    select_cols += f"NVL({column_name},to_date('01/01/1900', 'mm/dd/yyyy')) AS {column_name}"
                elif data_type == 'TIME':
                    select_cols += f"NVL({column_name},time('00:00:00')) AS {column_name}"
                elif data_type in ['DATETIME', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ']:
                    select_cols += f"NVL({column_name},to_timestamp('01/01/1900 00:00:00', 'mm/dd/yyyy hh24:mi:ss')) AS {column_name}"
                elif data_type in ['VARIANT', 'OBJECT']:
                    select_cols += f"NVL({column_name},parse_json('{{}}')) AS {column_name}"
                elif data_type == 'ARRAY':
                    select_cols += f"NVL({column_name},to_array('')) AS {column_name}"
                else:
                    select_cols += f"NVL({column_name},'') AS {column_name}"

        # For any columns added to new
        for (column_name, data_type, is_nullable) in inspect_tgt.get_table_alter_ddl(src=other, tgt=self, set_operator='MINUS', src_conn=inspect_src.connection, tgt_conn=inspect_tgt.connection):
            #print(column_name + ', ' + data_type + ',' + is_nullable)
            if is_nullable != 'YES':
                if len(insert_cols) > 0:
                    insert_cols += ', '
                    select_cols += ', '

                insert_cols += column_name
                if data_type in ['NUMBER', 'DECIMAL', 'NUMERIC', 'INT', 'INTEGER',
                                 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT',
                                 'FLOAT', 'FLOAT4', 'FLOAT8',
                                 'DOUBLE', 'DOUBLE PRECISION', 'REAL']:
                    select_cols += f"-1 AS {column_name}"
                elif data_type in ['VARCHAR', 'CHAR', 'CHARACTER', 'STRING', 'TEXT',
                                   'BINARY', 'VARBINARY']:
                    select_cols += f"'#' AS {column_name}"
                elif data_type == 'BOOLEAN':
                    select_cols += f"false AS {column_name}"
                elif data_type == 'DATE':
                    select_cols += f"to_date('01/01/1900', 'mm/dd/yyyy') AS {column_name}"
                elif data_type == 'TIME':
                    select_cols += f"time('00:00:00') AS {column_name}"
                elif data_type in ['DATETIME', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ']:
                    select_cols += f"to_timestamp('01/01/1900 00:00:00', 'mm/dd/yyyy hh24:mi:ss') AS {column_name}"
                elif data_type in ['VARIANT', 'OBJECT']:
                    select_cols += f"parse_json('{{}}') AS {column_name}"
                elif data_type == 'ARRAY':
                    select_cols += f"to_array('') AS {column_name}"
                else:
                    select_cols += f"'' AS {column_name}"

        sql_stmt += '\n\n' + \
            f"INSERT INTO {self.object_name}_INT ({insert_cols}) SELECT {select_cols} FROM {self.object_name};"

        sql_stmt += '\n\n' + \
            f"ALTER TABLE {self.object_name}_INT SWAP WITH {self.object_name};"

        sql_stmt += '\n\n' + f"DROP TABLE {self.object_name}_INT;"

        return sql_stmt


if __name__ == '__main__':

    diff = DeepDiff({"CHARACTER_MAXIMUM_LENGTH": 16, "CHARACTER_OCTET_LENGTH": 60, "COLUMN_NAME": "C_PHONE",
                     "DATA_TYPE": "TEXT", "IS_IDENTITY": "NO", "IS_NULLABLE": "NO", "IS_SELF_REFERENCING": "NO",
                     "ORDINAL_POSITION": 2},
                    {"CHARACTER_MAXIMUM_LENGTH": 16, "CHARACTER_OCTET_LENGTH": 60, "COLUMN_NAME": "C_PHONE",
                     "DATA_TYPE": "TEXT", "IS_IDENTITY": "NO", "IS_NULLABLE": "NO", "IS_SELF_REFERENCING": "NO",
                     "ORDINAL_POSITION": 2})
    print(diff)
    print(diff.get("values_changed"))
    print(diff.get("values_changed").get(
        "root['CHARACTER_MAXIMUM_LENGTH']").get("new_value"))
    print(diff.get("values_changed").get("root['CHARACTER_OCTET_LENGTH']"))
