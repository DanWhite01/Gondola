from src.db_objects.snowflake.database_object import DatabaseObject
from typing import Dict


class Procedure(DatabaseObject):
    """
    Representation of a database procedure
    """
    def __init__(self, attributes: Dict):

        self.argument_signature = attributes['ARGUMENT_SIGNATURE']
        DatabaseObject.__init__(self, attributes=attributes)

    def stripped_arguments(self) -> str:
        """
        Removes parameter names to create a argument signature
        :return: String of the argument signature
        """
        data_types = ['VARCHAR', 'TEXT', 'NUMBER', 'DOUBLE', 'FLOAT', 'BOOLEAN', 'DATE', 'TIMESTAMP_TZ',
                      'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'OBJECT', 'ARRAY', 'VARIANT', 'BINARY']

        stripped_arguments = self.argument_signature.replace('(', '').replace(')', '')
        words = stripped_arguments.split()
        for word in words:
            if word.upper() not in data_types:
                words.remove(word)
        result = ' '.join(words)
        return result

    def get_ddl_sql(self):
        """
        get_ddl with the argument signature included
        :return: SQL statement for the get_ddl
        """
        sql = f'select get_ddl(\'{self.get_object_type()}\', \'"{self.schema_name}".{self.object_name}({self.stripped_arguments()})\')'
        return sql

    def create_alter_statement(self, other, include_drops):
        return other.create_sql()