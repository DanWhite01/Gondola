import argparse
import configparser
#from src import non_rest
import non_rest
from util.dropped_objects_exception import DroppedObjectException
from util.str_to_user_credentials import str2usercreds
# michael test git


print('started')
# Create the parser and add arguments

# Read local `config.ini` file.
config = configparser.ConfigParser()
config.read('config/gondola_cli_config.ini')

# Get values from our .ini file
config_arg = []

for each_section in config.sections():
    print(each_section)
    for (each_key, each_val) in config.items(each_section):
        if each_val:
            config_arg.append({each_section: each_key.upper()})
print(config_arg)


# exit()

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


parser = argparse.ArgumentParser()
parser.add_argument('-a', '--author', dest='AUTHOR',
                    required=True, help="Author")
parser.add_argument('-tn', '--ticket_nbr', dest='TICKET_NBR',
                    required=True, help="Ticket Number")
parser.add_argument('-d', '--directory', dest='WIN_DIR', required=True,
                    help="<directoryname><String> where directory name for location of deployment files <Required>")
parser.add_argument('-t', '--target_creds', dest='TGT_CREDS',
                    required=True, help="Username:Password@acct:DBNAME")
parser.add_argument('-s', '--source_creds', dest='SRC_CREDS',
                    required=True, help="Username:Password@acct:DBNAME")
parser.add_argument('-tw',  '--tgt_warehouse',
                    dest='TGT_WAREHOUSE', required=True, help="Warehouse")
parser.add_argument('-sw',  '--src_warehouse',
                    dest='SRC_WAREHOUSE', required=True, help="Warehouse")
parser.add_argument('-tauth', '--tgt_auth', dest='TGT_AUTH',
                    required=True, help="authenticator")
parser.add_argument('-sauth', '--src_auth', dest='SRC_AUTH',
                    required=True, help="authenticator")
parser.add_argument('-id', '--include_drops', dest='ID',
                    type=str2bool, nargs='?', const=True, default=False)
parser.add_argument('-es', '--exclude_schema', dest='EX_SCHEMA',
                    help="<excludeschema><String> where single schema name or comma list of schemas are excluded from compare <optional>")


# Reset `required` attribute when provided from config file
for action in parser._actions:
    # for every dictionary in list
    for d in config_arg:
        # get the key value of the dict. the key is the section and the value is the parameter
        for key, val in d.items():
            if action.dest in val:
                action.required = False
                action.default = config[key][val]

args = parser.parse_args()

target_database, source_database, output_directory, exclude_schema, ticket_number, author, include_drops = '', '', '', '', '', '', False

# print(args.TGT_CREDS)
tgt_credentials = str2usercreds(args.TGT_CREDS)
src_credentials = str2usercreds(args.SRC_CREDS)


if args.WIN_DIR:
    output_directory = args.WIN_DIR
if args.EX_SCHEMA:
    exclude_schema = args.EX_SCHEMA
if args.ID:
    include_drops = args.ID
if args.TICKET_NBR:
    ticket_number = args.TICKET_NBR
if args.AUTHOR:
    author = args.AUTHOR
if args.TGT_WAREHOUSE:
    tgt_credentials['warehouse'] = args.TGT_WAREHOUSE
if args.SRC_WAREHOUSE:
    src_credentials['warehouse'] = args.SRC_WAREHOUSE
if args.TGT_AUTH:
    tgt_credentials['authenticator'] = args.TGT_AUTH
if args.SRC_AUTH:
    src_credentials['authenticator'] = args.SRC_AUTH

migration = non_rest.Migration(source_db=src_credentials,
                               target_db=tgt_credentials,
                               exclude_schema=exclude_schema,
                               file_directory=output_directory,
                               ticket_number=ticket_number,
                               author=author)
try:
    migration.create_files()
except DroppedObjectException as err:
    if include_drops:
        print("ignore exception for dropped objects and this has been bypassed")
        pass
    else:
        print("Dropped Files have been created. Exception Raised.")
        raise DroppedObjectException

