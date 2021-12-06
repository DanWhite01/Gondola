from flask import Flask, request
from flask_restful import Api, Resource
from flask_cors import CORS
from src.connectors.snowflake_service import SnowflakeService
from contextlib import contextmanager
from compare import Compare
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)
cors = CORS(app, resources={
    r"/*": {
        "origins": "*"
    }
})
api = Api(app)


connection_src = {
    'user': os.getenv('SF_SRC_USER'),
    'password': os.getenv('SF_SRC_PASSWORD'),
    'database': os.getenv('SF_SRC_DATABASE'),
    'account': os.getenv('SF_SRC_ACCOUNT'),
    'warehouse': os.getenv('SF_SRC_WAREHOUSE'),
    'role': os.getenv('SF_SRC_ROLE'),
    'authenticator': os.getenv('SF_SRC_AUTHENTICATOR')}

connection_tgt = {
    'user': os.getenv('SF_TGT_USER'),
    'password': os.getenv('SF_TGT_PASSWORD'),
    'database': os.getenv('SF_TGT_DATABASE'),
    'account': os.getenv('SF_TGT_ACCOUNT'),
    'warehouse': os.getenv('SF_TGT_WAREHOUSE'),
    'role': os.getenv('SF_TGT_ROLE'),
    'authenticator': os.getenv('SF_TGT_AUTHENTICATOR')}

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


def refresh_database_names_list(conn):

    conn = conn
    # To get database names we issue 'show databases' then follow up with a command to pick up the results
    conn.cursor().execute("SHOW DATABASES")
    sql = 'SELECT "name" as DATABASE_NAME FROM TABLE(result_scan(last_query_id()));'
    cursor = conn.cursor().execute(sql)

    all_database_names = []

    for row in cursor:
        all_database_names.append(row[0])

    return all_database_names

class DBCompare(Resource):

    def put(self, id):

        global in_tgt_not_src
        global in_src_not_tgt
        global in_both_update
        global no_differences
        global object_types_in_scope

        global json_string

        if id == 6:
            # Pass in Objects to compare
            object_array = request.form["object"]
            object_types_in_scope = object_array.split(",")
        elif id == 0:
            # Default Return All JSON results from comparison
            src_db_name = request.form["src_db"]
            tgt_db_name = request.form["tgt_db"]
            object_types_in_scope = request.form["object"].split(",")

            # logger.info("Objects in scope" + object_types_in_scope)

            connection_src['database'] = src_db_name
            connection_tgt['database'] = tgt_db_name

            json_string = Compare(connection_src, connection_tgt,
                                  include_drops=True, db_object_types_to_include=object_types_in_scope)


            result_dict = json_string.__dict__

            # compare_databases returns the following populated json

            in_src_not_tgt = []
            for x in result_dict['json_to_tgt']:
                if x['_action'] == 'NEW':
                    in_src_not_tgt.append(x)

            in_tgt_not_src = []
            for x in result_dict['json_to_tgt']:
                if x['_action'] == 'DROP':
                    in_tgt_not_src.append(x)

            in_both_update = []
            for x in result_dict['json_to_tgt']:
                if x['_action'] == 'MODIFY':
                    in_both_update.append(x)

            no_differences = []
            for x in result_dict['json_to_tgt']:
                if x['_action'] == 'NO CHANGE':
                    no_differences.append(x)

            return result_dict['json_to_tgt']

    def get(self, id):
        if id == 1:
            # In Target Not in Source
            return in_tgt_not_src
        elif id == 2:
            # In Source Not in Target
            return in_src_not_tgt
        elif id == 3:
            # In Both
            return in_both_update
        elif id == 4:
            # Not Change
            return no_differences
        elif id == 5:
            # Get Database List
            with connection_context(connection_src) as conn_src:
                get_all_database_names = refresh_database_names_list(conn_src)
            return get_all_database_names
        elif id == 99:
            # Default Return All JSON results from comparison
            # return json.loads(json_string)
            result_dict2 = json_string.__dict__
            return result_dict2['json_to_tgt']

        result_dict2 = json_string.__dict__

        return result_dict2['json_to_tgt']

api.add_resource(DBCompare, "/compare/<int:id>")

if __name__ == "__main__":
    app.run(debug=True)


