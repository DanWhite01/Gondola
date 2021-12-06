from src.db_objects.snowflake.database_object import DatabaseObject
from typing import Dict

class View(DatabaseObject):

    def __init__(self, attributes: Dict):
        DatabaseObject.__init__(self, attributes=attributes)

    def create_alter_statement(self, other, include_drops):
        return other.create_sql()
