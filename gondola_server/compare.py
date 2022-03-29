from src.connectors.snowflake_service import SnowflakeService
from src.inspect import Inspect
from src.dependancy_mapper import Object_Depend
from typing import Dict, List
from contextlib import contextmanager
from util.property_yaml_reader import read_properties_yaml
import json
import logging.config
from util.setup_logging import setup_logging
import os
from dotenv import load_dotenv

load_dotenv()

# Read config and sett class variables
compare_properties = read_properties_yaml('config/compare_properties.yml')
config_db_object_types_to_include = compare_properties.get(
    'db_object_types_to_include')

alterable_db_objects = ['tables']

setup_logging()

logger = logging.getLogger(__name__)


@contextmanager
def connection_context(connection):
    """
    Yields a snowflake connection context
    :param connection: Dictionary of connection details
    """
    if connection:
        with SnowflakeService(**connection).conn as s:
            yield s
    else:
        yield None


class Compare(object):
    """

    """

    def __init__(self, connection_src: Dict, connection_tgt: Dict, include_drops: bool = False,
                 schema_to_exclude: List = None, db_object_types_to_include: List = None):

        self.connection_src = connection_src
        self.connection_tgt = connection_tgt
        self.include_drops = include_drops
        self.schema_to_exclude = schema_to_exclude
        self.db_object_types_to_include = db_object_types_to_include or config_db_object_types_to_include

        self.json_to_tgt = []

        with connection_context(connection_src) as conn_src, connection_context(connection_tgt) as conn_tgt:
            self.inspection_src = Inspect(
                conn=conn_src, db_object_types_to_include=self.db_object_types_to_include)
            self.inspection_tgt = Inspect(
                conn=conn_tgt, db_object_types_to_include=self.db_object_types_to_include)

            # For each object type that needs to be compared,
            # excluding columns as they will be compared for tables if necessary
            for db_object_to_compare in self.db_object_types_to_include:
                if db_object_to_compare != 'columns':
                    self.compare_db_objects(db_object_to_compare)

        dependancy_order = self.json_to_tgt

        #print(json.dumps(dependancy_order, indent=4))

        unique_ordered_list = Object_Depend(dependancy_order).order()

        # print(json.dumps(unique_ordered_list, indent=4))

        self.json_to_tgt = unique_ordered_list

    def get_json_to_tgt(self, action: None, db_object_type: None):
        return_json = []
        if action:
            for item in self.json_to_tgt:
                if item.action == action:
                    return_json.append(item)
        else:
            return_json += self.json_to_tgt

        return return_json

    def compare_db_objects(self, db_object_type):

        objects_in_src = self.inspection_src.get_db_objects_by_type(
            db_object_type)
        objects_in_tgt = self.inspection_tgt.get_db_objects_by_type(
            db_object_type)

        objects_in_both_diff_ddl = []
        objects_in_both_no_diff = []
        objects_not_in_target = []
        objects_not_in_source = []

        # Used to compare objects by name
        objects_in_tgt_by_name = [item.object_name for item in objects_in_tgt]
        objects_in_src_by_name = [item.object_name for item in objects_in_src]

        # fetch objects for each scenario and populate lists
        for src_obj in objects_in_src:
            if not objects_in_tgt:
                print('Target is Empty')
                if src_obj.object_name not in objects_in_tgt_by_name and src_obj not in objects_not_in_target:
                    src_obj.diff_ddl = {
                        'src_db': self.connection_src['database'], 'src_ddl': src_obj.original_ddl, 'tgt_db': self.connection_tgt['database'], 'tgt_ddl': ''}
                    objects_not_in_target.append(src_obj)

            for tgt_obj in objects_in_tgt:
                # fetch objects_in_both_diff_ddl
                if src_obj.object_name == tgt_obj.object_name and src_obj.is_ddl_different(tgt_obj):
                    tgt_obj.diff_ddl = {'src_db': self.connection_src['database'], 'src_ddl': src_obj.original_ddl,
                                        'tgt_db': self.connection_tgt['database'], 'tgt_ddl': tgt_obj.original_ddl}
                    # tgt_obj.change_ddl = tgt_obj.is_ddl_different(src_obj)
                    objects_in_both_diff_ddl.append(tgt_obj)
                # fetch objects_in_both_no_diff
                if src_obj == tgt_obj:
                    tgt_obj.diff_ddl = {'src_db': self.connection_src['database'], 'src_ddl': src_obj.original_ddl,
                                        'tgt_db': self.connection_tgt['database'], 'tgt_ddl': tgt_obj.original_ddl}
                    objects_in_both_no_diff.append(tgt_obj)
                # fetch objects in source and not in target
                if src_obj.object_name not in objects_in_tgt_by_name and src_obj not in objects_not_in_target:
                    src_obj.diff_ddl = {
                        'src_db': self.connection_src['database'], 'src_ddl': src_obj.original_ddl, 'tgt_db': self.connection_tgt['database'], 'tgt_ddl': ''}
                    objects_not_in_target.append(src_obj)
                # fetch objects in target and not in source
                if tgt_obj.object_name not in objects_in_src_by_name and tgt_obj not in objects_not_in_source:
                    tgt_obj.diff_ddl = {'src_db': self.connection_src['database'], 'src_ddl': '',
                                        'tgt_db': self.connection_tgt['database'], 'tgt_ddl': tgt_obj.original_ddl}
                    objects_not_in_source.append(tgt_obj)

        # Failsafe option where object only exists in Target
        for tgt_obj in objects_in_tgt:
            if tgt_obj.object_name not in objects_in_src_by_name and tgt_obj not in objects_not_in_source:
                tgt_obj.diff_ddl = {'src_db': self.connection_src['database'], 'src_ddl': '',
                                    'tgt_db': self.connection_tgt['database'], 'tgt_ddl': tgt_obj.original_ddl}
                objects_not_in_source.append(tgt_obj)

        for obj in objects_in_both_diff_ddl:
            print('objects_in_both_diff_ddl', obj.object_name)

        for obj in objects_in_both_no_diff:
            print('objects_in_both_no_diff', obj.object_name)

        for obj in objects_not_in_target:
            print('objects_not_in_target', obj.object_name)

        for obj in objects_not_in_source:
            print('objects_not_in_source', obj.object_name)

        for db_object in objects_not_in_target:
            db_object.database_name = self.connection_tgt.get('database')
            if db_object_type == 'schema':
                db_object.change_ddl = db_object.original_ddl
            else:
                db_object.change_ddl = db_object.use_schema_sql() + db_object.original_ddl
            db_object.original_ddl = None
            db_object.action = 'NEW'
            db_object.db_object_type = db_object_type
            self.json_to_tgt.append(db_object.__dict__)

        # Generate drops
        if self.include_drops and db_object_type != 'column':
            for db_object in objects_not_in_source:
                if db_object_type == 'schema':
                    db_object.change_ddl = db_object.drop_sql()
                else:
                    db_object.change_ddl = db_object.use_schema_sql() + db_object.drop_sql()
                db_object.action = 'DROP'
                db_object.db_object_type = db_object_type
                self.json_to_tgt.append(db_object.__dict__)

        # Modified
        for db_object in objects_in_both_diff_ddl:
            for src_object in objects_in_src:
                if db_object.object_name == src_object.object_name:
                    if db_object_type == 'tables':
                        db_object.change_ddl = db_object.create_alter_statement(
                            src_object, self.inspection_tgt, self.inspection_src)
                    else:
                        db_object.change_ddl = db_object.create_alter_statement(
                            src_object, self.include_drops)
                        print(db_object.change_ddl)

            # print(db_object.__dict__)
            db_object.database_name = self.connection_tgt.get('database')
            db_object.action = 'MODIFY'
            db_object.db_object_type = db_object_type
            self.json_to_tgt.append(db_object.__dict__)

        # Unchanged
        for db_object in objects_in_both_no_diff:
            db_object.database_name = self.connection_tgt.get('database')
            db_object.change_ddl = ""
            db_object.original_ddl = None
            db_object.action = 'NO CHANGE'
            db_object.db_object_type = db_object_type
            self.json_to_tgt.append(db_object.__dict__)

    def create_json_entry(self):
        True

    def populate_columns(self, tables_to_alter):
        self.inspection_tgt.populated_columns(table_names=tables_to_alter)
        self.inspection_src.populated_columns(table_names=tables_to_alter)


if __name__ == '__main__':
    test_connection_src = {
        'user': os.getenv('SF_SRC_USER'),
        'password': os.getenv('SF_SRC_PASSWORD'),
        'database': os.getenv('SF_SRC_TEST_DEPLOY_DB'),
        'account': os.getenv('SF_SRC_ACCOUNT'),
        'warehouse': os.getenv('SF_SRC_WAREHOUSE'),
        'role': os.getenv('SF_SRC_ROLE'),
        'authenticator': os.getenv('SF_SRC_AUTHENTICATOR')}

    test_connection_tgt = {
        'user': os.getenv('SF_TGT_USER'),
        'password': os.getenv('SF_TGT_PASSWORD'),
        'database': os.getenv('SF_TGT_TEST_DEPLOY_DB'),
        'account': os.getenv('SF_TGT_ACCOUNT'),
        'warehouse': os.getenv('SF_TGT_WAREHOUSE'),
        'role': os.getenv('SF_TGT_ROLE'),
        'authenticator': os.getenv('SF_TGT_AUTHENTICATOR')}

    Compare(connection_src=test_connection_src,
            connection_tgt=test_connection_tgt, include_drops=True)
