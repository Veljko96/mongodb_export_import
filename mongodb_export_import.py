import pymongo
import os
import json
import argparse

parser = argparse.ArgumentParser(description='This is python script for exporting from and importing in mongodb')
parser.add_argument('-f','--host', help='Mongodb host', default='localhost', required=True)
parser.add_argument('-o', '--port', help='Mongodb port', default=27017, type=int, required=True)
parser.add_argument('-u','--username',help='Mongodb user', default='root', required=True)
parser.add_argument('-p', '--password', help='Mongodb password', required=True)
parser.add_argument('-d', '--database', help='Mongodb database', required=True)
parser.add_argument('-l', '--database_new', help="Mongodb database for exported data", required=True)
parser.add_argument('-s', '--start_time', help='Start time for exporting', required=True)
parser.add_argument('-e', '--end_time', help='End time for exporting', required=True)
parser.add_argument('-m', '--month_number', help='Number of month', required=True)
parser.add_argument('-t', '--output', help='Directory for output', required=True)
parser.add_argument('-c', '--column_name', help='Column name for timestamp', required=True)
parser.add_argument('--nargs', nargs='+')

args = parser.parse_args()

host = args.host
port = args.port
username = args.username
password = args.password
database = args.database
database_new = args.database_new
start_time = args.start_time
end_time = args.end_time
month_number = args.month_number
column_name = args.column_name
output = args.output
list_of_vehicles = args.nargs

client = pymongo.MongoClient(host=host, port=port, maxPoolSize=50, username=username, password=password, authSource="admin")

for collection in client[database].collection_names():
	if "ampq" in collection or "mqtt" in collection:
		#missing part for vehicles, when you add it, indent everything after 40 line(for loop for vehicles)
		#for vehicle in list_of_vehicles:
        query_string = "--query='{" + f'''"{column_name}":''' + " {" + f'''"$gte": {start_time}, "$lte": {end_time}''' + "} }'"
        export_command = f'''mongoexport --username {username} --password {password} --host {host}:{port} --authenticationDatabase admin --db {database} --collection {collection} ''' + query_string + f''' --out {output}'''
        os.system(export_command)

        new_collection = collection + "_" + month_number
        import_command = f'''mongoimport --username {username} --password {password} --host {host}:{port} --authenticationDatabase admin --db {database_new} --collection {new_collection} --file {output}'''
        os.system(import_command)
            
        os.system(f"rm {output}")
