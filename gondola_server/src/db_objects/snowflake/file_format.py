from src.db_objects.snowflake.database_object import DatabaseObject
from typing import Dict
from deepdiff import DeepDiff

class File_Format(DatabaseObject):

    def __init__(self, attributes: Dict):
        DatabaseObject.__init__(self, attributes=attributes)
        self.attributes_to_alter = {}
        self.update_attributes = ['RECORD_DELIMITER' , 'FIELD_DELIMITER', 'SKIP_HEADER','DATE_FORMAT','TIME_FORMAT','TIMESTAMP_FORMAT','BINARY_FORMAT','ESCAPE','ESCAPE_UNENCLOSED_FIELD','FIELD_OPTIONALLY_ENCLOSED_BY','NULL_IF','COMPRESSION, COMMENT']

    def create_alter_statement(self, other, include_drops):
        # sql_stmt = ''
        #
        # # Drop Seq Statment
        # if include_drops:
        #     sql_stmt += f'DROP FILE FORMAT {self.object_name};'
        #
        # # Get differerences between two dicts
        # diff = DeepDiff(self.attributes, other)
        # if diff:
        #     self.attributes_to_alter = diff.get('values_changed')
        #
        # for root, value in self.attributes_to_alter.items():
        #     # TODO: extend this to cover all alter and add scenarios
        #     for attr in self.update_attributes:
        #         if attr in root:
        #             sql_stmt += self.use_schema_sql() + f'ALTER FILE FORMAT {self.object_name} SET {attr} =  {value.get("new_value")};' \
        #
        #     if 'COMMENT' in root:
        #         if not value.get("new_value"):
        #             # Question for Mike do we need to enclose quotes for comment?
        #             sql_stmt += self.use_schema_sql() + f'ALTER FILE FORMAT {self.object_name} SET COMMENT = {value.get("new_value")};'
        #         else:
        #             sql_stmt += self.use_schema_sql() + f'ALTER FILE FORMAT  {self.object_name} UNSET COMMENT;'


        # Get differerences between two dicts
        return other.create_sql()



