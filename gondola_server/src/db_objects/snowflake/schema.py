from src.db_objects.snowflake.database_object import DatabaseObject
from typing import Dict


class Schema(DatabaseObject):
    """

    """

    def __init__(self, attributes: Dict):
        DatabaseObject.__init__(self, attributes=attributes)

        self.transient = " TRANSIENT" if attributes['IS_TRANSIENT'] == 'YES' else ""
        self.with_managed_access = " WITH MANAGED ACCESS" if attributes[
            'IS_MANAGED_ACCESS'] == 'YES' else ""
        #self.with_managed_access = 'ENABLE' if attributes['IS_MANAGED_ACCESS'] == 'YES' else 'DISABLE'
        self.retention_time = attributes['RETENTION_TIME']
        attributes['DDL'] = self.create_ddl()

        DatabaseObject.__init__(self, attributes=attributes)

    def get_ddl_sql(self):
        """
        get_ddl statement for the object
        :return: get_ddl statement for the object
        """
        return "select 'no get ddl call for schema';"

    def create_ddl(self):
        """
        get_ddl statement for the object
        :return: get_ddl statement for the object
        """

        comment = f" COMMENT = '{self.comment}'" if self.comment != 'None' else ""

        return f'CREATE{self.transient} SCHEMA {self.object_name}{self.with_managed_access}{comment};\n'

    '''
         DATA_RETENTION_TIME_IN_DAYS
         MAX_DATA_EXTENSION_TIME_IN_DAYS (NOT IN SQL)
         DEFAULT_DDL_cOLLATION (NOT IN SQL)
         MANAGED_ACCESS
         COMMENT'''

    def create_alter_statement(self, other, include_drops):
        sql_stmt = self.use_schema_sql()
        # Check if Comment matches
        if self.comment != other.comment:
            if sql_stmt[-1] != '\n':
                sql_stmt += '\n'

            # If Comment is null or blank need to UNSET
            if (not other.comment) or (other.comment!=""):
                sql_stmt += f"ALTER SCHEMA {self.object_name} SET COMMENT = '{other.comment}';"
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