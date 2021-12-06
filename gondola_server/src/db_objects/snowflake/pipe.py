from src.db_objects.snowflake.database_object import DatabaseObject
from typing import Dict


class Pipe(DatabaseObject):

    def __init__(self, attributes: Dict):

        self.definition = attributes['DEFINITION']
        DatabaseObject.__init__(self, attributes=attributes)


    def create_alter_statement(self, other, include_drops):
        sql_stmt = self.use_schema_sql()

        # Check if Comment matches
        if self.comment != other.comment:
            if sql_stmt[-1] != '\n':
                sql_stmt += '\n'

            # If Comment is null or blank need to UNSET
            if not other.comment:
                sql_stmt += f"ALTER SCHEMA {self.object_name} SET COMMENT = '{other.comment}'';"
            else:
                sql_stmt += f'ALTER SCHEMA {self.object_name} UNSET COMMENT;'

        # Check if increment matches
        if self.retention_time != other.retention_time:
            if sql_stmt[-1] != '\n':
                sql_stmt += '\n'

            # If retention_time is null or blank need to UNSET
            if not other.retention_time:
                sql_stmt += f'ALTER SCHEMA {self.object_name} SET DATA_RETENTION_TIME_IN_DAYS = {other.retention_time};'
            else:
                sql_stmt += f'ALTER SCHEMA {self.object_name} UNSET DATA_RETENTION_TIME_IN_DAYS;'

        # Check Managed Access
        if self.with_managed_access != other.with_managed_access:
            if sql_stmt[-1] != '\n':
                sql_stmt += '\n'

            sql_stmt += f'ALTER SCHEMA {self.object_name} {other.with_managed_access } MANAGED ACCESS;'

        return sql_stmt
