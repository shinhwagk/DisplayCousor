import argparse

from xplan.display_cursor import display_cursor

parser = argparse.ArgumentParser()
parser.add_argument('-dsn', help="db name", required=True)
parser.add_argument('-sql_id',  help="inst", required=True)
parser.add_argument('-child_number',  help="data source", default=0)
parser.add_argument('-print', action='store_true')
args = parser.parse_args()

display_cursor(args. dsn, args.sql_id, args.child_number).print()
