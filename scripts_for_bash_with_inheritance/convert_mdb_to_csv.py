# import jaydebeapi
# import os
# import sys
# import csv
#
# db_path = os.path.abspath(sys.argv[1])
#
# ucanaccess_jars = [
#     f"{os.environ.get('XL_IDP_ROOT_FTS')}/jdbc/ucanaccess-5.0.1.jar",
#     f"{os.environ.get('XL_IDP_ROOT_FTS')}/jdbc/commons-lang3-3.8.1.jar",
#     f"{os.environ.get('XL_IDP_ROOT_FTS')}/jdbc/commons-logging-1.2.jar",
#     f"{os.environ.get('XL_IDP_ROOT_FTS')}/jdbc/hsqldb-2.5.0.jar",
#     f"{os.environ.get('XL_IDP_ROOT_FTS')}/jdbc/jackcess-3.0.1.jar"
# ]
# classpath = ":".join(ucanaccess_jars)
# cnxn = jaydebeapi.connect("net.ucanaccess.jdbc.UcanaccessDriver",
#                           f"jdbc:ucanaccess://{db_path};newDatabaseVersion=V2010", ["", ""], classpath)
# crsr = cnxn.cursor()
# crsr.execute(f"select * from {os.path.basename(sys.argv[1]).replace('.mdb', '')}")
# with open(f'{os.environ.get("XL_IDP_PATH_FTS")}/flat_fts/csv/{os.path.basename(sys.argv[1])}.csv', 'w') as csvfile:
#     writer = csv.writer(csvfile)
#     writer.writerow([i[0] for i in crsr.description])
#     writer.writerows(crsr.fetchall())
# cnxn.commit()
# crsr.close()
# cnxn.close()


import csv
import os
import sys
from mdb_parser import MDBTable

input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]
file_name_without_exp = os.path.basename(sys.argv[1]).replace(".mdb", "")

table = MDBTable(file_path=input_file_path, table=file_name_without_exp)
columns = table.columns
print(columns)
csv_table = []
for rows in table:
    dict_data = {column: row for row, column in zip(rows, columns)}
    csv_table.append(dict_data)

csv_file = f"{sys.argv[2]}/{os.path.basename(sys.argv[1])}.csv"
try:
    with open(csv_file, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for data in csv_table:
            writer.writerow(data)
    print("File was written successfully")
except IOError:
    print("I/O error")
