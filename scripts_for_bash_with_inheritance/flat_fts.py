import os
import re
import sys
import json
import datetime
import itertools
import contextlib
import numpy as np
from __init__ import *
from typing import Generator
from pandas import DataFrame, read_csv


class FTS(object):
    def __init__(self, filename: str, folder: str):
        self.filename: str = filename
        self.folder: str = folder

    @staticmethod
    def divide_chunks(list_data: list, chunk: int) -> Generator:
        """
        Divide by chunks of a list.
        """
        for i in range(0, len(list_data), chunk):
            yield list_data[i:i + chunk]

    @staticmethod
    def convert_format_date(date: str) -> str:
        """
        Convert to a date type.
        """
        if date_of_registration := re.findall(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{1,2}[.]\d{1,2}[.]\d{2,4}', date):
            for date_format in date_formats:
                with contextlib.suppress(ValueError):
                    return str(datetime.datetime.strptime(date_of_registration[0], date_format).date())
        return date

    @staticmethod
    def convert_to_int(int_value: str) -> int:
        """
        Convert a value to integer.
        """
        with contextlib.suppress(ValueError):
            return int(int_value)

    @staticmethod
    def rename_columns(df: DataFrame) -> None:
        """
        Rename of a columns.
        """
        dict_columns_eng: dict = {}
        for column, columns in itertools.product(df.columns, headers_eng):
            for column_eng in columns:
                if column == column_eng:
                    dict_columns_eng[column] = headers_eng[columns]
        df.rename(columns=dict_columns_eng, inplace=True)

    def convert_csv_to_dict(self) -> list:
        """
        Csv data representation in json.
        """
        df: DataFrame = read_csv(self.filename, low_memory=False, dtype=str)
        df.replace({np.NAN: None}, inplace=True)
        df = df.dropna(axis=0, how='all')
        df = df.dropna(axis=1, how='all')
        self.rename_columns(df)
        return df.to_dict('records')

    def change_type(self, data: dict) -> None:
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
                    data[key] = self.convert_format_date(value)
                elif key in list_of_int_type:
                    data[key] = self.convert_to_int(value)

    def save_data_to_file(self, i: int) -> None:
        """
        Save a data to a file.
        """
        basename: str = os.path.basename(self.filename)
        output_file_path: str = os.path.join(self.folder, f'{basename}_{i}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(chunk_parsed_data, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    input_file_path: str = os.path.abspath(sys.argv[1])
    output_folder: str = sys.argv[2]
    fts: FTS = FTS(input_file_path, output_folder)
    parsed_data: list = fts.convert_csv_to_dict()
    original_file_index: int = 0
    divided_parsed_data: list = list(fts.divide_chunks(parsed_data, 50000))
    for index, chunk_parsed_data in enumerate(divided_parsed_data):
        for dict_data in chunk_parsed_data:
            fts.change_type(dict_data)
            dict_data['original_file_name'] = os.path.basename(input_file_path)
            dict_data['original_file_parsed_on'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            dict_data['original_file_index'] = original_file_index
            original_file_index += 1
        fts.save_data_to_file(index)
