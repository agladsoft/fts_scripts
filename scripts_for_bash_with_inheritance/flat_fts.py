import os
import sys
import json
import datetime
import itertools
import contextlib
import numpy as np
import pandas as pd
from __init__ import *


def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def rename_columns(df):
    dict_columns_eng = {}
    for column, columns in itertools.product(df.columns, headers_eng):
        for column_eng in columns:
            if column == column_eng:
                dict_columns_eng[column] = headers_eng[columns]
    df.rename(columns=dict_columns_eng, inplace=True)


input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]

df = pd.read_csv(input_file_path, low_memory=False, dtype=str)
df.replace({np.NAN: None}, inplace=True)
rename_columns(df)
parsed_data = df.to_dict('records')

original_file_index = 0
divided_parsed_data = list(divide_chunks(parsed_data, 50000))
for index, chunk_parsed_data in enumerate(divided_parsed_data):
    for dict_data in chunk_parsed_data:
        for key, value in dict_data.items():
            with contextlib.suppress(Exception):
                if key in ['the_total_customs_value_of_the_gtd', 'total_invoice_value_for_gtd', 'currency_exchange_rate',
                           'number_of_goods_in_additional_units', 'the_number_of_goods_in_the_second_unit_change',
                           'net_weight_kg', 'gross_weight_kg', 'invoice_value', 'customs_value_rub', 'statistical_cost_usd',
                           'usd_for_kg', 'quota']:
                    dict_data[key] = float(value)
                elif key in ['stat']:
                    if value in ['True', 'False']:
                        dict_data[key] = str(int(value == 'True'))
        dict_data['original_file_name'] = os.path.basename(input_file_path)
        dict_data['original_file_parsed_on'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        dict_data['original_file_index'] = original_file_index
        original_file_index += 1
    basename = os.path.basename(input_file_path)
    output_file_path = os.path.join(output_folder, f'{basename}_{index}.json')
    with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
        json.dump(chunk_parsed_data, f, ensure_ascii=False, indent=4)
