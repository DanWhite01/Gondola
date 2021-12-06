from src.db_objects.snowflake.database_object import DatabaseObject
from typing import Dict

class Sequence(DatabaseObject):
    
    def __init__(self, attributes: Dict):
        DatabaseObject.__init__(self, attributes=attributes)
        self.sequences = []
        self.increment = attributes['INCREMENT']

    def create_alter_statement(self, other, include_drops):
        sql_stmt = self.use_schema_sql()

        # Drop Seq Statment
        # if include_drops:
        #     sql_stmt += f'DROP SEQUENCE {self.object_name};'

        print(self.object_name+', '+self.comment+', '+self.increment)
        print(other.object_name+', '+other.comment+', '+other.increment)
        # Check if Comment matches

        if self.comment != other.comment:
            if sql_stmt[-1] != '\n':
                sql_stmt += '\n'
            # If Comment is null or blank need to UNSET
            if not other.comment:
                # Question for Mike do we need to enclose quotes for comment?
                sql_stmt += f"ALTER SEQUENCE {self.object_name} SET COMMENT = '{other.comment}';"
            else:
                sql_stmt += f'ALTER SEQUENCE {self.object_name} UNSET COMMENT;'

        # Check if increment matches
        if self.increment != other.increment:
            if sql_stmt[-1] != '\n':
                sql_stmt += '\n'

            sql_stmt += f'ALTER SEQUENCE {self.object_name} SET INCREMENT = {other.increment};'

        return sql_stmt













