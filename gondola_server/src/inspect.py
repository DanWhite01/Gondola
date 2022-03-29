from snowflake.connector import DictCursor
from src.database_object_factory import create_database_object, available_object_types
from typing import Dict, List
from src.sql_file_creator import SqlTemplateEnvironment, SqlFileCreator
import ast
import re

# Register SQL files from templates
information_schema_sql_templates = SqlTemplateEnvironment(
    searchpath='templates/snowflake/information_schema')
sql_files = SqlFileCreator()

custom_sql_templates = SqlTemplateEnvironment(
    searchpath='templates/snowflake/custom_sql')


class Inspect(object):
    """
    Fetches all the objects for a database connection
    """

    def __init__(self, conn, db_object_types_to_include: List):

        self.connection = conn
        self.db_object_types_to_include = db_object_types_to_include

        # Create SQL templates
        for db_type in self.db_object_types_to_include:
            template_name = information_schema_sql_templates.get_template(
                f'{db_type}.j2')
            sql_files.register_template(
                key=f'{db_type}', template=template_name)

        self.all_db_objects = []

        for db_object in self.db_object_types_to_include:
            print("*** Comparing *** -> " + db_object)
            self.all_db_objects += self.fetch_objects(db_object)

    def get_db_objects_by_type(self, db_object_type):
        """
        Returns a List of objects from the database objects list for a given type of db object
        """
        return_objects = []

        for db_object in self.all_db_objects:
            if isinstance(db_object, available_object_types.get(db_object_type)):

                # remove database name from the ddl as its causing differences
                pattern = r"(\w*)\.(\w*)\.(?!NEXTVAL)"
                replace = "\\2."

                print('Before')
                print(db_object.original_ddl)

                db_object.original_ddl = re.sub(
                    pattern, replace, db_object.original_ddl, 0)
                return_objects.append(db_object)

                print('After')
                print(db_object.original_ddl)

        return return_objects

    def fetch_objects(self, object_type: str):
        """
        Creates database objects from the dictionary returned by the database connection
        """

        sql_statements = sql_files.create_sql_file(key=object_type.lower(),
                                                   parameters=None).split(';')

        sql_statements_len = len(sql_statements)-1

        for index, x in enumerate(sql_statements):
            if index == sql_statements_len:
                # print(str(x))
                inspected_objects = self.connection.cursor(
                    DictCursor).execute(x).fetchall()
                # print(inspected_objects)
            else:
                # print(str(x))
                self.connection.cursor().execute(x).fetchall()

        db_objects = []

        for db_object in inspected_objects:
            new_db_object = create_database_object(object_type=object_type,
                                                   attributes=db_object)
            # add ddl to new database object
            if object_type == 'schema':
                new_db_object.original_ddl = self.schema_create_ddl(
                    db_object=new_db_object)
            else:
                new_db_object.original_ddl = self.get_ddl(
                    db_object=new_db_object)

            db_objects.append(new_db_object)

        return db_objects

    def populated_columns(self, table_names):

        for table_name in table_names:
            for table in self.get_db_objects_by_type('tables'):
                if table.object_name == table_name.object_name:

                    inspected_columns = self.connection.cursor(DictCursor).execute(
                        sql_files.create_sql_file(key='columns',
                                                  parameters={'database_name': table.database_name,
                                                              'schema_name': table.schema_name,
                                                              'object_name': table.object_name})).fetchall()

                    for column in inspected_columns:
                        column_attributes = ast.literal_eval(
                            column['COLUMN_ATTRIBUTES'])
                        column_attributes['SCHEMA_NAME'] = table.schema_name
                        column_attributes['DATABASE_NAME'] = table.database_name
                        column_attributes['TABLE_NAME'] = table.object_name
                        new_column_object = create_database_object(object_type='columns',
                                                                   attributes=column_attributes)
                        table.columns.append(new_column_object)

    def get_ddl(self, db_object):
        sql = db_object.get_ddl_sql()
        ddl = self.connection.cursor().execute(sql).fetchone()[0]
        return ddl

    def schema_create_ddl(self, db_object):
        sql = db_object.create_ddl()
        return sql

    def get_table_alter_ddl(self, src, tgt, set_operator, src_conn, tgt_conn):

        col_list = []
        # Generate column list
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        custom_sql_type = 'list_tab_cols'
        template_name = custom_sql_templates.get_template(
            f'{custom_sql_type}.j2')
        sql_files.register_template(
            key=f'{custom_sql_type}', template=template_name)

        src_cur.execute(
            sql_files.create_sql_file(key=custom_sql_type,
                                      parameters={'database_name': src.database_name,
                                                  'schema_name': src.schema_name,
                                                  'object_name': src.object_name}))
        src_sfqid = src_cur.sfqid

        tgt_cur.execute(
            sql_files.create_sql_file(key=custom_sql_type,
                                      parameters={'database_name': tgt.database_name,
                                                  'schema_name': tgt.schema_name,
                                                  'object_name': tgt.object_name}))
        tgt_sfqid = tgt_cur.sfqid

        custom_sql_type = 'table_col_compare'
        template_name = custom_sql_templates.get_template(
            f'{custom_sql_type}.j2')
        sql_files.register_template(
            key=f'{custom_sql_type}', template=template_name)

        src_cols = src_cur.execute(
            sql_files.create_sql_file(key=custom_sql_type,
                                      parameters={'sfqid': src_sfqid,
                                                  'database_name': src.database_name})).fetchall()
        tgt_cols = tgt_cur.execute(
            sql_files.create_sql_file(key=custom_sql_type,
                                      parameters={'sfqid': tgt_sfqid,
                                                  'database_name': tgt.database_name})).fetchall()

        if set_operator == 'INTERSECT':
            for src_col in src_cols:
                print(src_col)
                for tgt_col in tgt_cols:
                    if src_col[0] == tgt_col[0] and src_col[1] == tgt_col[1]:
                        col_list.append(src_col)
        elif set_operator == 'MINUS':
            for src_col in src_cols:
                matched = False
                for tgt_col in tgt_cols:
                    if src_col[0] == tgt_col[0] and src_col[1] == tgt_col[1]:
                        matched = True
                    
                if not matched:
                    col_list.append(src_col)
        else:
            col_list = src_cols

        return col_list
