from typing import Dict
from src.sql_file_creator import SqlFileCreator
from src.sql_file_creator import SqlTemplateEnvironment
from compare import Compare
from datetime import datetime
import os
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()


object_types_in_scope = ['schema', 'tables', 'views',
                         'columns', 'sequences', 'procedures', 'functions']

# Need to compare and create files locally on system.
# Need to then do Gondola_CLI which call Non Rest with input paramters


class Migration(object):

    def __init__(self, source_db: Dict, target_db: Dict, file_directory, ticket_number, exclude_schema='', author=None):

        self.snowflake_service_src = source_db
        self.snowflake_service_tgt = target_db

        self.exclude_schema = exclude_schema
        self.file_directory = file_directory
        self.ticket_number = ticket_number
        self.author = author

        self.files_to_deploy = []
        self.drop_files_created = False

    def create_files(self):

        # create files using compare.py to get the JSON returned
        # just pass connection details into Compare
        print('Create Files')

        # Set Variables
        file_directory = self.file_directory
        author = self.author
        ticket_number = self.ticket_number
        connection_src = self.snowflake_service_src
        connection_tgt = self.snowflake_service_tgt
        files_to_deploy = []

        # capture return from compare
        json_string = Compare(connection_src, connection_tgt,
                              include_drops=True, db_object_types_to_include=object_types_in_scope)

        print('Returned JSON')

        result_dict = json_string.__dict__
        print(result_dict['json_to_tgt'])

        information_schema_sql_templates = SqlTemplateEnvironment(
            searchpath='templates/output')
        sql_files = SqlFileCreator()

        for change in result_dict['json_to_tgt']:

            if change['_action'] != 'NO CHANGE':

                last_modified = change['last_altered']
                action = change['_action']
                file_name = change["object_name"].lower() + ".sql"

                if change['_action'] == 'NEW' and change['db_object_type'] != 'columns':
                    # construct the file path
                    filepath = "ddl/" + change["schema_name"].lower() + \
                        "/" + change["db_object_type"].lower() + "/" + \
                        change["object_name"].lower() + ".sql"

                    full_file_name = Path(file_directory).joinpath(filepath)

                    db_type = 'create_sql'

                    template_name = information_schema_sql_templates.get_template(
                        f'{db_type}.j2')
                    sql_files.register_template(
                        key=f'{db_type}', template=template_name)

                    # At this point the create template has been registered.
                    # Now need to pass into the template the json object then render it

                    os.makedirs(os.path.dirname(full_file_name), exist_ok=True)

                    with open(full_file_name, 'w') as file:
                        file.write(sql_files.create_sql_file(
                            parameters=change, key=db_type.lower()))
                    file.close()

                if change['_action'].upper() == 'MODIFY' or change['_action'].upper() == 'DROP' or (
                    change['_action'].upper(
                    ) == 'NEW' and change['db_object_type'] == 'columns'
                ):
                    # construct the file path
                    filepath = "alter_scripts/" + change["schema_name"].lower() + \
                        "/" + change["db_object_type"].lower() + "/" + \
                        change["object_name"].lower() + ".sql"

                    full_file_name = Path(file_directory).joinpath(filepath)

                    db_type = 'alter_sql'

                    template_name = information_schema_sql_templates.get_template(
                        f'{db_type}.j2')
                    sql_files.register_template(
                        key=f'{db_type}', template=template_name)

                    # At this point the alter template has been registered.
                    # Now need to pass into the template the json object then render it

                    os.makedirs(os.path.dirname(full_file_name), exist_ok=True)

                    with open(full_file_name, 'w') as file:
                        file.write(sql_files.create_sql_file(
                            parameters=change, key=db_type.lower()))
                    file.close()

                create_id = f'{last_modified[0:19].replace("-", "").replace(":", "").replace(" ", "")}_{file_name}_{action}'

                files_to_deploy.append({"file_location": filepath,
                                        "id": create_id})

        files_to_deploy.append({"ticket_number": ticket_number,
                                "author": author})

        manifest_directory = file_directory + "/manifest_files/"

        manifest_filename = f"{ticket_number.lower()}.yaml"

        full_file_name = manifest_directory + manifest_filename

        db_type = 'manifest'

        template_name = information_schema_sql_templates.get_template(
            f'{db_type}.j2')
        sql_files.register_template(
            key=f'{db_type}', template=template_name)

        # At this point the create template has been registered.
        # Now need to pass into the template the json object then render it

        os.makedirs(os.path.dirname(full_file_name), exist_ok=True)

        with open(full_file_name, 'w') as file:
            file.write(sql_files.create_sql_file(
                parameters=files_to_deploy, key=db_type.lower()))
            file.close()


if __name__ == '__main__':
    connection_src = {
        'user': os.getenv('SF_SRC_USER'),
        'password': os.getenv('SF_SRC_PASSWORD'),
        'database': os.getenv('SF_SRC_TEST_DEPLOY_DB'),
        'account': os.getenv('SF_SRC_ACCOUNT'),
        'warehouse': os.getenv('SF_SRC_WAREHOUSE'),
        'role': os.getenv('SF_SRC_ROLE'),
        'authenticator': os.getenv('SF_SRC_AUTHENTICATOR')}

    connection_tgt = {
        'user': os.getenv('SF_TGT_USER'),
        'password': os.getenv('SF_TGT_PASSWORD'),
        'database': os.getenv('SF_TGT_TEST_DEPLOY_DB'),
        'account': os.getenv('SF_TGT_ACCOUNT'),
        'warehouse': os.getenv('SF_TGT_WAREHOUSE'),
        'role': os.getenv('SF_TGT_ROLE'),
        'authenticator': os.getenv('SF_TGT_AUTHENTICATOR')}

    exclude_schema = ''

    output_directory = os.getenv('GONDOLA_OUTPUT_DIRECTORY') #'C:\\gtest'

    ticket_number = 'JIRA-12345'

    author = 'DWHITE'

    Migration(source_db=connection_src,
              target_db=connection_tgt,
              file_directory=output_directory,
              exclude_schema=exclude_schema,
              ticket_number=ticket_number,
              author=author).create_files()
