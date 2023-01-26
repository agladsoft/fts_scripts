import os
import re
import sys
import json
import datetime
import itertools
import contextlib
import numpy as np
import pandas as pd
from __init__ import *


def divide_chunks(l, n):
    """
    Divide by chunks of a list.
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def convert_format_date(date):
    """
    Convert to a date type.
    """
    if date_of_registration := re.findall(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{1,2}[.]\d{1,2}[.]\d{2,4}', date):
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                return datetime.datetime.strptime(date_of_registration[0], date_format).date()
    return date


def convert_to_int(int_value):
    """
    Convert a value to integer.
    """
    with contextlib.suppress(ValueError):
        return int(int_value)


def rename_columns(df):
    """
    Rename of a columns.
    """
    dict_columns_eng = {}
    for column, columns in itertools.product(df.columns, headers_eng):
        for column_eng in columns:
            if column == column_eng:
                dict_columns_eng[column] = headers_eng[columns]
    df.rename(columns=dict_columns_eng, inplace=True)


def save_data_to_file(input_file, folder):
    """
    Save a data to a file.
    """
    basename = os.path.basename(input_file)
    output_file_path = os.path.join(folder, f'{basename}_{index}.json')
    with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
        json.dump(chunk_parsed_data, f, ensure_ascii=False, indent=4)


def change_type(data):
    """
    Change a type of data.
    """
    for key, value in data.items():
        with contextlib.suppress(Exception):
            if key in list_of_float_type:
                data[key] = float(value)
            elif key in list_of_str_type:
                if value in ['True', 'False']:
                    data[key] = str(int(value == 'True'))
            elif key in list_of_date_type:
                data[key] = convert_format_date(value)
            elif key in list_of_int_type:
                data[key] = convert_to_int(value)


def convert_csv_to_dict(filename):
    """
    Csv data representation in json.
    """
    df = pd.read_csv(filename, low_memory=False, dtype=str)
    df.replace({np.NAN: None}, inplace=True)
    rename_columns(df)
    return df.to_dict('records')


if __name__ == "__main__":
    input_file_path = os.path.abspath(sys.argv[1])
    output_folder = sys.argv[2]
    parsed_data = convert_csv_to_dict(input_file_path)
    original_file_index = 0
    divided_parsed_data = list(divide_chunks(parsed_data, 50000))
    for index, chunk_parsed_data in enumerate(divided_parsed_data):
        for dict_data in chunk_parsed_data:
            change_type(dict_data)
            dict_data['original_file_name'] = os.path.basename(input_file_path)
            dict_data['original_file_parsed_on'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            dict_data['original_file_index'] = original_file_index
            original_file_index += 1
        save_data_to_file(input_file_path, output_folder)
