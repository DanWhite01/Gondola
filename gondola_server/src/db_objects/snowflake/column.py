from src.db_objects.snowflake.database_object import DatabaseObject
import random
import string

text_type_fields = ['VARCHAR', 'CHAR', 'TEXT', 'CHARACTER', 'STRING']

class Column(DatabaseObject):
    """
    Representation of a database column
    Column attributes are stored in a Dictionary
    """



    def __init__(self, attributes):

        self.column_attributes = attributes
        self.attributes_to_alter = {}
        self.drop_flag = False
        self.table_name = attributes['TABLE_NAME']
        self.temp_column_name = ''
        self.data_type = attributes['DATA_TYPE']
        DatabaseObject.__init__(self, attributes=attributes)

        del self.column_attributes['DATABASE_NAME']

    def __eq__(self, other):
        if not isinstance(other, Column):
            return 'Object types are not the same'

        return self.object_name == other.object_name and self.column_attributes == other.column_attributes

    def drop_column_sql_suffix(self):
        if self.drop_flag:
            return f'DROP COLUMN {self.object_name}'

    def create_alter_statement(self, include_drops):
        sql_stmt = self.use_schema_sql() + f"ALTER TABLE {self.table_name} "
        if self.drop_flag and include_drops:
            sql_stmt += self.drop_column_sql_suffix()
        else:
            sql_stmt += self.alter_column_sql_suffix()

        self.change_ddl = sql_stmt + ';'

    def create_alter_column(self):
        sql_stmt = f'ALTER TABLE IF EXISTS {self.table_name}'
        column_attributes = ['CHARACTER_MAXIMUM_LENGTH', 'NUMERIC_PRECISION', 'DATA_TYPE', 'NUMERIC_SCALE', 'COLLATION_NAME']
        # for root, value in self.attributes_to_alter.items():
        #     # TODO: extend this to cover all alter and add scenarios
        #     print(root, value)

        char_max = '' if "root['CHARACTER_MAXIMUM_LENGTH']" not in self.attributes_to_alter \
            else f""" ({int(self.attributes_to_alter.get("root['CHARACTER_MAXIMUM_LENGTH']").get("new_value"))})"""

        num_pro = '' if "root['NUMERIC_PRECISION']" not in self.attributes_to_alter \
            else f""" ({int(self.attributes_to_alter.get("root['NUMERIC_PRECISION']").get("new_value"))}"""

        num_scl = '' if "root['NUMERIC_SCALE']" not in self.attributes_to_alter \
            else f""" , {int(self.attributes_to_alter.get("root['NUMERIC_SCALE']").get("new_value"))}"""

        collate = '' if "root['COLLATION_NAME']" not in self.attributes_to_alter \
            else f""" COLLATE {self.attributes_to_alter.get("root['COLLATION_NAME']").get("new_value")}"""

        expression = '' if "root['EXPRESSION']" not in self.attributes_to_alter \
            else f""" as '{self.attributes_to_alter.get("root['EXPRESSION']").get("new_value")}'"""

        comment = '' if "root['EXPRESSION']" not in self.attributes_to_alter \
            else f"""\nCOMMENT '{self.attributes_to_alter.get("root['COMMENT']").get("new_value")}'"""

        nullable = '' if "root['IS_NULLABLE']" == 'YES' \
            else f""" -- NOT NULL """

        if self.data_type == 'NUMBER':
            sql_stmt += f'ADD COLUMN {self.object_name} {self.data_type}{num_pro}{num_scl}{collate}{expression}{comment}'
        elif self.data_type in text_type_fields:
            sql_stmt += f'ADD COLUMN {self.object_name} {self.data_type}{char_max}{collate}{expression}{comment}'
        else:
            sql_stmt += f'ADD COLUMN {self.object_name} {self.data_type}{collate}{expression}{comment}'
        return sql_stmt + ';'

    def _generate_random_suffix(self, length):
        '''
        Generates a random suffix to create a temporary column
        '''
        letters = string.ascii_uppercase
        return ''.join(random.choice(letters) for i in range(length))

    def _create_temp_column(self):
        """
        Populate self.temp_column_name and returns the SQL to alter the column name
        """
        self.temp_column_name = self.object_name + '_' + self._generate_random_suffix(length=10)

        return f'ALTER TABLE {self.table_name} RENAME COLUMN {self.object_name} to {self.temp_column_name};'

    def _rollback_temp_rename(self):
        """
        Returns the SQL to rollback a column rename in event of a failure
        """
        if self.temp_column_name is None:
            return f'Temp column for {self.database_name}.{self.object_name} does not exist'
        else:
            return f'ALTER TABLE {self.table_name} RENAME COLUMN {self.temp_column_name} to {self.object_name};'

    def check_column_exist(self, column):
        return self.column_attributes.get(column)