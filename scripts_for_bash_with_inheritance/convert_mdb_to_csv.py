import jaydebeapi
import os
import sys
import csv

db_path = os.path.abspath(sys.argv[1])

ucanaccess_jars = [
    f"{os.environ.get('XL_IDP_ROOT_FTS')}/jdbc/ucanaccess-5.0.1.jar",
    f"{os.environ.get('XL_IDP_ROOT_FTS')}/jdbc/commons-lang3-3.8.1.jar",
    f"{os.environ.get('XL_IDP_ROOT_FTS')}/jdbc/commons-logging-1.2.jar",
    f"{os.environ.get('XL_IDP_ROOT_FTS')}/jdbc/hsqldb-2.5.0.jar",
    f"{os.environ.get('XL_IDP_ROOT_FTS')}/jdbc/jackcess-3.0.1.jar"
]
classpath = ":".join(ucanaccess_jars)
cnxn = jaydebeapi.connect("net.ucanaccess.jdbc.UcanaccessDriver",
                          f"jdbc:ucanaccess://{db_path};newDatabaseVersion=V2010", ["", ""], classpath)
crsr = cnxn.cursor()
crsr.execute(f"select * from {os.path.basename(sys.argv[1]).replace('.mdb', '')} limit 100")
with open(f'{os.environ.get("XL_IDP_PATH_FTS")}/flat_fts/csv/{os.path.basename(sys.argv[1])}.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([i[0] for i in crsr.description])
    writer.writerows(crsr.fetchall())
cnxn.commit()
crsr.close()
cnxn.close()

