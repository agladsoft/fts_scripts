import csv
import os
import logging
import sys
import json
import datetime

if not os.path.exists("logging"):
    os.mkdir("logging")

logging.basicConfig(filename=f"logging/{os.path.basename(__file__)}.log", level=logging.DEBUG)
log = logging.getLogger()


def merge_two_dicts(x, y):
    z = x.copy()   # start with keys and values of x
    z.update(y)    # modifies z with keys and values of y
    return z


def isDigit(x):
    try:
        float(x)
        return True
    except ValueError:
        return False


class Fts(object):

    def __init__(self):
        pass

    def process(self, input_file_path):
        logging.info(f'file is {os.path.basename(input_file_path)} {datetime.datetime.now()}')
        with open(input_file_path, newline='') as csvfile:
            lines = list(csv.DictReader(csvfile))

        logging.info(f'lines type is {type(lines)} and contain {len(lines)} items')
        logging.info(f'First 3 items are: {lines[:3]}')

        fileds_to_get = ['id', 'date_of_registration2']

        parsed_data = []
        for line in lines:
            new_line = {k: v.strip() for k, v in line.items() if k in fileds_to_get}
            parsed_data.append(new_line)

        return parsed_data


input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, f'{basename}.json')
print(f"output_file_path is {output_file_path}")

parsed_data = Fts().process(input_file_path)

with open(output_file_path, 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)
 