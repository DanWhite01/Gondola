from typing import Dict

actions = ["DROP", "NEW", "MODIFY", "NO CHANGE"]


class DatabaseObject(object):
    """
    Parent object for database objects. This will never be instantiated but has common attributes for child objects
    """

    def __init__(self, attributes: Dict):

        self.database_name = attributes['DATABASE_NAME']
        self.schema_name = attributes['SCHEMA_NAME']
        self.object_name = attributes['OBJECT_NAME']

        self.original_ddl = attributes.get('DDL') or ''
        self.change_ddl = ''

        self._action = ''

        self.comment = attributes.get('COMMENT') or ''
        self.last_altered = str(attributes.get('LAST_ALTERED')) or ''

    def __eq__(self, other):
        if not isinstance(other, DatabaseObject):
            return 'Object types are not the same'

        return self.schema_name == other.schema_name and self.original_ddl == other.original_ddl

    @property
    def action(self):
        return self._action

    @action.setter
    def action(self, new_action):
        if new_action.upper() in actions:
            self._action = new_action
        else:
            raise ValueError(f'Invalid Action: {new_action}')
        

    def is_ddl_different(self, other):
        """
        Checks if the DDL is different between two objects with the same name
        :param other: Object instance of the object to compare to this instance
        :return: Boolean
        """
        if not isinstance(other, DatabaseObject):
            return 'Object types are not the same'

        return self.schema_name == other.schema_name and self.object_name == other.object_name \
               and self.original_ddl != other.original_ddl

    def drop_sql(self):
        """
        Create the Drop statement for the object
        :return: String of the Drop statement
        """
        if self.get_object_type() == 'SCHEMA':
            return f'DROP {self.get_object_type()} IF EXISTS {self.schema_name};'
        elif self.get_object_type() != 'SCHEMA' and self.object_name is None:
            raise Exception('Object name is empty')
        else:
            return f'DROP {self.get_object_type()} IF EXISTS "{self.schema_name}"."{self.object_name}";'

    def create_sql(self):
        """
        Create the Create statement for the object
        :return: String of the Create statement
        """
        return self.use_schema_sql() + self.original_ddl

    def use_schema_sql(self):
        """
        Use SCHEMA statement for the object
        :return: SQL statement to set the schema for the object
        """
        return f'USE SCHEMA {self.schema_name};\n'

    def get_object_type(self):
        """
        Return the object type derived from the object class name
        :return: String of the object type in UPPER case
        """
        return self.__class__.__name__.upper()

    def get_ddl_sql(self):
        """
        get_ddl statement for the object
        :return: get_ddl statement for the object
        """
        sql = f'select get_ddl(\'{self.get_object_type()}\', \'"{self.schema_name}"."{self.object_name}"\')'
        return sql

    def create_dict_for_json_entry(self):
        json = self.__dict__
        json['CREATE_DDL'] = self.create_sql()
        json['DROP_SQL'] = self.drop_sql()
        return json
