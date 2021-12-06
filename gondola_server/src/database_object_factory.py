from src.db_objects.snowflake.table import Table
from src.db_objects.snowflake.view import View
from src.db_objects.snowflake.column import Column
from src.db_objects.snowflake.procedure import Procedure
from src.db_objects.snowflake.function import Function
from src.db_objects.snowflake.schema import Schema
from src.db_objects.snowflake.file_format import File_Format
from src.db_objects.snowflake.sequence import Sequence
from src.db_objects.snowflake.pipe import Pipe
from src.db_objects.snowflake.task import Task
from src.db_objects.snowflake.stream import Stream

# Register and map the database objects
available_object_types = {'tables': Table,
                          'views': View,
                          'procedures': Procedure,
                          'functions': Function,
                          'columns': Column,
                          'schema': Schema,
                          'file_formats': File_Format,
                          'sequences': Sequence,
                          'pipes': Pipe,
                          'streams': Stream,
                          'tasks': Task}


def create_database_object(object_type, attributes):
    """
    Creates a database object from the object type and a dictionary of the objects attributes
    :param object_type: string representation of the object type
    :param attributes: Dictionary of the attributes for teh object
    :return: A database object with the attributes set
    """
    return available_object_types[object_type](attributes=attributes)