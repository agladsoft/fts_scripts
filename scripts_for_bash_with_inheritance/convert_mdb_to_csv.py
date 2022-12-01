import os
import csv
import sys
from mdb_parser import MDBTable, MDBParser
from __init__ import logger

basename_input_file_path = os.path.basename(sys.argv[1])
dir_name_input_file_path = os.path.dirname(sys.argv[1])
input_file_path = f"{dir_name_input_file_path}/'{basename_input_file_path}'"
output_folder = sys.argv[2]
file_name_without_exp = os.path.basename(sys.argv[1]).replace(".mdb", "")

db = MDBParser(file_path=input_file_path)
logger.info(f"File {input_file_path} will be read")
table = MDBTable(file_path=input_file_path, table=db.tables[0])
columns = table.columns + ["original_file_index"]
csv_table = []
for index, rows in enumerate(table):
    dict_data = {column: row for row, column in zip(rows, columns)}
    dict_data["original_file_index"] = index
    csv_table.append(dict_data)

csv_file = f"{sys.argv[2]}/{os.path.basename(sys.argv[1])}.csv"
logger.info(f"File {input_file_path} will be write in directory {csv_file}")
try:
    with open(csv_file, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        writer.writerows(csv_table)
    logger.info(f"File {input_file_path} was written successfully")
except IOError:
    print("I/O error")
