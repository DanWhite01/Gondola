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
                    diff = DeepDiff(column.column_attributes, other_column.column_attributes)
                    if diff:
                        column.attributes_to_alter = diff.get('values_changed')
                        column_alter_scripts.append(column.create_alter_column())

        for column in self.columns:
            if column.object_name not in matched_columns:
                column.drop_flag = True

        return column_alter_scripts

    def create_alter_statement(self, other, include_drops, conn):

        ##DDL to create a new interim table with the new structure.
        src_ddl = other.original_ddl.replace(self.object_name, self.object_name+'_INT')
        sql_stmt = self.use_schema_sql() + '\n' + src_ddl 

        # Now write the sql to populate this new table with the data from old table
        # Any new columns which are defined as not null, are to be handled through NVL with default values

        #Generate column list
        cur = conn.cursor()
        cur.execute(f'SHOW COLUMNS IN {other.database_name}.{other.schema_name}.{other.object_name}')
        src_sfqid = cur.sfqid
        #print(src_sfqid)
        cur.execute(f'SHOW COLUMNS IN {self.database_name}.{self.schema_name}.{self.object_name}')
        tgt_sfqid = cur.sfqid
        #print(tgt_sfqid)

        src_table_sql = f"""SELECT b.column_name, b.data_type, b.is_nullable 
                             FROM TABLE(RESULT_SCAN('{src_sfqid}')) a 
                             JOIN {other.database_name}.information_schema.columns b
                               ON (a."database_name" = b.table_catalog
                              AND a."schema_name" = b.table_schema
                              AND a."table_name" = b.table_name
                              AND a."column_name" = b.column_name)
                            WHERE a."kind" != 'VIRTUAL_COLUMN'"""

        tgt_table_sql = f"""SELECT b.column_name, NVL(c.data_type,b.data_type) data_type, NVL(c.is_nullable,b.is_nullable) is_nullable 
                             FROM TABLE(RESULT_SCAN('{tgt_sfqid}')) a
                             JOIN {self.database_name}.information_schema.columns b
                               ON (a."database_name" = b.table_catalog
                              AND a."schema_name" = b.table_schema
                              AND a."table_name" = b.table_name
                              AND a."column_name" = b.column_name)
                             LEFT JOIN {other.database_name}.information_schema.columns c
                               ON (b.table_schema = c.table_schema
                              AND b.table_name = c.table_name
                              AND b.column_name = c.column_name)
                            WHERE a."kind" != 'VIRTUAL_COLUMN'"""

        #src_table_sql = """SELECT "column_name" AS column_name, "data_type" AS data_type FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2))) WHERE "kind" != 'VIRTUAL_COLUMN'"""
        #tgt_table_sql = """SELECT "column_name" AS column_name, "data_type" AS data_type FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1))) WHERE "kind" != 'VIRTUAL_COLUMN'"""

        #final_sql = f"""SELECT lower(column_name) column_name, data_type FROM ({src_table_sql} INTERSECT {tgt_table_sql}) """
        insert_cols = ""
        select_cols = ""

        ## For any column modifications or drops from new
        for idx, (column_name, data_type, is_nullable) in enumerate(conn.cursor().execute(f"""SELECT lower(column_name) column_name, data_type, is_nullable FROM ({src_table_sql} INTERSECT {tgt_table_sql}) """)):
            #print(column_name + ', ' + data_type + ',' + is_nullable)
            if idx > 0:
                insert_cols += ', ' 
                select_cols += ', ' 
            
            insert_cols += column_name
            if is_nullable == 'YES':
                select_cols += column_name
            else:
                if data_type in ['NUMBER','DECIMAL', 'NUMERIC','INT', 'INTEGER', 
                                              'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT', 
                                              'FLOAT', 'FLOAT4', 'FLOAT8',
                                              'DOUBLE', 'DOUBLE PRECISION', 'REAL']:
                    select_cols +=  f"NVL({column_name},-1) AS {column_name}"
                elif data_type in ['VARCHAR','CHAR', 'CHARACTER','STRING', 'TEXT', 
                                              'BINARY', 'VARBINARY']:
                    select_cols +=  f"NVL({column_name},'#') AS {column_name}"
                elif data_type == 'BOOLEAN':
                    select_cols +=  f"NVL({column_name},false) AS {column_name}"
                elif data_type == 'DATE':
                    select_cols +=  f"NVL({column_name},to_date('01/01/1900', 'mm/dd/yyyy')) AS {column_name}"
                elif data_type == 'TIME':
                    select_cols +=  f"NVL({column_name},time('00:00:00')) AS {column_name}"
                elif data_type in ['DATETIME','TIMESTAMP', 'TIMESTAMP_LTZ','TIMESTAMP_NTZ', 'TIMESTAMP_TZ']:
                    select_cols +=  f"NVL({column_name},to_timestamp('01/01/1900 00:00:00', 'mm/dd/yyyy hh24:mi:ss')) AS {column_name}"
                elif data_type in ['VARIANT','OBJECT']:
                    select_cols +=  f"NVL({column_name},parse_json('{{}}')) AS {column_name}"
                elif data_type == 'ARRAY':
                    select_cols +=  f"NVL({column_name},to_array('')) AS {column_name}"
                else:
                    select_cols +=  f"NVL({column_name},'') AS {column_name}"

        ## For any columns added to new
        for (column_name, data_type, is_nullable) in conn.cursor().execute(f"""SELECT lower(column_name) column_name, data_type, is_nullable FROM ({src_table_sql} MINUS {tgt_table_sql}) """):
            #print(column_name + ', ' + data_type + ',' + is_nullable)
            if is_nullable != 'YES':
                if len(insert_cols) > 0:
                    insert_cols += ', ' 
                    select_cols += ', ' 
            
                insert_cols += column_name
                if data_type in ['NUMBER','DECIMAL', 'NUMERIC','INT', 'INTEGER', 
                                              'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT', 
                                              'FLOAT', 'FLOAT4', 'FLOAT8',
                                              'DOUBLE', 'DOUBLE PRECISION', 'REAL']:
                    select_cols +=  f"-1 AS {column_name}"
                elif data_type in ['VARCHAR','CHAR', 'CHARACTER','STRING', 'TEXT', 
                                              'BINARY', 'VARBINARY']:
                    select_cols +=  f"'#' AS {column_name}"
                elif data_type == 'BOOLEAN':
                    select_cols +=  f"false AS {column_name}"
                elif data_type == 'DATE':
                    select_cols +=  f"to_date('01/01/1900', 'mm/dd/yyyy') AS {column_name}"
                elif data_type == 'TIME':
                    select_cols +=  f"time('00:00:00') AS {column_name}"
                elif data_type in ['DATETIME','TIMESTAMP', 'TIMESTAMP_LTZ','TIMESTAMP_NTZ', 'TIMESTAMP_TZ']:
                    select_cols +=  f"to_timestamp('01/01/1900 00:00:00', 'mm/dd/yyyy hh24:mi:ss') AS {column_name}"
                elif data_type in ['VARIANT','OBJECT']:
                    select_cols +=  f"parse_json('{{}}') AS {column_name}"
                elif data_type == 'ARRAY':
                    select_cols +=  f"to_array('') AS {column_name}"
                else:
                    select_cols +=  f"'' AS {column_name}"

        sql_stmt += '\n\n' + f"INSERT INTO {self.object_name}_INT ({insert_cols}) SELECT {select_cols} FROM {self.object_name};"

        sql_stmt += '\n\n' + f"ALTER TABLE {self.object_name}_INT SWAP WITH {self.object_name};"

        sql_stmt += '\n\n' + f"DROP TABLE {self.object_name}_INT;"

        return sql_stmt
        # sql_stmt = self.use_schema_sql() + f'ALTER TABLE IF EXISTS {self.object_name}'
        # alter_statements = []

        # if self.comment != other.comment:
        #     alter_statements.append(f'{sql_stmt} SET COMMENT = {other.comment};')

        # if self.compare_column_attr(other=other):

        #     if column.drop_flag and include_drops:
        #         sql_stmt += column.drop_column_sql_suffix()
        #     else:
        #         sql_stmt += column.alter_column_sql_suffix()

        #     self.change_ddl = sql_stmt + ';'

        # return alter_statements

if __name__ == '__main__':

    diff = DeepDiff({"CHARACTER_MAXIMUM_LENGTH": 16, "CHARACTER_OCTET_LENGTH": 60, "COLUMN_NAME": "C_PHONE",
                  "DATA_TYPE": "TEXT", "IS_IDENTITY": "NO", "IS_NULLABLE": "NO", "IS_SELF_REFERENCING": "NO",
                  "ORDINAL_POSITION": 2},
                 {"CHARACTER_MAXIMUM_LENGTH": 16, "CHARACTER_OCTET_LENGTH": 60, "COLUMN_NAME": "C_PHONE",
                  "DATA_TYPE": "TEXT", "IS_IDENTITY": "NO", "IS_NULLABLE": "NO", "IS_SELF_REFERENCING": "NO",
                  "ORDINAL_POSITION": 2})
    print(diff)
    print(diff.get("values_changed"))
    print(diff.get("values_changed").get("root['CHARACTER_MAXIMUM_LENGTH']").get("new_value"))
    print(diff.get("values_changed").get("root['CHARACTER_OCTET_LENGTH']"))