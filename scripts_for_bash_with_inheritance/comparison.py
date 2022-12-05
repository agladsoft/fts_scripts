import os
import sys
import glob
import zipfile
import hashlib
import itertools
import contextlib
import numpy as np
import pandas as pd
from pandas import DataFrame
from typing import List, Tuple
from __init__ import headers_eng, logger


class Zip(object):
    def __init__(self, zip_file):
        self.zip_file: str = zip_file

    @staticmethod
    def save_files_for_zip(df_up: DataFrame, df_down: DataFrame, up_file: str, down_file: str, hash_up: str,
                           hash_down: str) -> None:
        with open(f"{up_file}.txt", "w") as hash_up_file:
            hash_up_file.write(hash_up)
        with open(f"{down_file}.txt", "w") as hash_down_file:
            hash_down_file.write(hash_down)
        df_up.to_csv(up_file, index=False)
        df_down.to_csv(down_file, index=False)

    def unzip_file(self) -> List[str]:
        with zipfile.ZipFile(self.zip_file, "r") as zf:
            return [zf.extract(name, f"{os.path.dirname(sys.argv[1])}/csv") for name in
                    sorted(zf.namelist(), reverse=True)]

    def zip_files(self, up_file: str, down_file: str) -> str:
        absolute_file_zip = f"{os.path.dirname(self.zip_file)}/done/{os.path.basename(self.zip_file)}_compared.zip"
        with zipfile.ZipFile(absolute_file_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(up_file, os.path.basename(up_file))
            zf.write(down_file, os.path.basename(down_file))
            zf.write(f"{up_file}.txt", os.path.basename(f"{up_file}.txt"))
            zf.write(f"{down_file}.txt", os.path.basename(f"{down_file}.txt"))
        return absolute_file_zip


class Csv(object):
    def __init__(self, up_file, down_file):
        self.upload_file = up_file
        self.download_file = down_file
        self.df_upload = None
        self.df_download = None
        self.hash_upload = None
        self.hash_download = None

    @staticmethod
    def change_types_columns_and_replace_quot_marks(parsed_data: List[dict]) -> None:
        for dict_data in parsed_data:
            for key, value in dict_data.items():
                with contextlib.suppress(Exception):
                    dict_data[key] = value.replace('"', '').replace("''", "")
                    if key in ['the_total_customs_value_of_the_gtd', 'total_invoice_value_for_gtd',
                               'currency_exchange_rate', 'number_of_goods_in_additional_units',
                               'the_number_of_goods_in_the_second_unit_change', 'net_weight_kg', 'gross_weight_kg',
                               'invoice_value', 'customs_value_rub', 'statistical_cost_usd', 'usd_for_kg', 'quota']:
                        dict_data[key] = str(float(value))
                    elif key in ['stat']:
                        if value in ['True', 'False']:
                            dict_data[key] = str(int(value == 'True'))

    def rename_columns(self) -> None:
        dict_columns_eng = {}
        for column, _columns in itertools.product(self.df_upload.columns, headers_eng):
            for column_eng in _columns:
                if column == column_eng:
                    dict_columns_eng[column] = headers_eng[_columns]
        self.df_upload.rename(columns=dict_columns_eng, inplace=True)

    def compare_csv(self) -> None:
        self.df_upload['flag'] = 'old'
        self.df_download['flag'] = 'new'
        df_concat: DataFrame = pd.concat([self.df_upload, self.df_download])
        duplicates_dropped: DataFrame = df_concat.drop_duplicates(df_concat.columns.difference(['flag']), keep=False)
        duplicates_dropped.to_csv(f'{os.path.dirname(sys.argv[1])}/csv/{os.path.basename(sys.argv[1])}_difference.csv',
                                  index=False)

    def is_equal_hash(self, up_file: str, down_file: str) -> None:
        if self.hash_download == self.hash_download:
            logger.info(f"EQUAL: The hashes of {up_file} and {down_file} are equal")
        else:
            logger.info(f"NOT EQUAL: The hashes of {up_file} and {down_file} are not equal")

    def get_same_columns_from_csv(self) -> list:
        self.df_upload: DataFrame = pd.read_csv(self.upload_file, low_memory=False, dtype=str)
        self.df_download: DataFrame = pd.read_csv(self.download_file, low_memory=False, dtype=str)
        self.rename_columns()
        same_columns = list(set(list(self.df_download.columns)).intersection(set(list(self.df_upload.columns))))
        return same_columns

    def read_csv_pandas(self, df: DataFrame, base_name_csv_file: str, same_columns) -> Tuple[DataFrame, str]:
        df: DataFrame = df.loc[:, df.columns.isin(same_columns)]
        df = df[same_columns]
        df.replace({np.NAN: None}, inplace=True)
        parsed_data: List[dict] = df.to_dict('records')
        logger.info(f"{base_name_csv_file}: Dataframe have been converted to a list with dictionary")
        self.change_types_columns_and_replace_quot_marks(parsed_data)
        logger.info(f"{base_name_csv_file}: Changed column types")
        df = pd.DataFrame(parsed_data)
        logger.info(f"{base_name_csv_file}: List with dictionary converted to a dataframe")
        hash_file: str = hashlib.md5(str(parsed_data).encode()).hexdigest()
        logger.info(f"{base_name_csv_file}: Hash is {hash_file}")
        return df, hash_file

    def main(self, basename_upload_file, basename_download_file):
        columns = self.get_same_columns_from_csv()
        self.df_upload, self.hash_upload = self.read_csv_pandas(self.df_upload, basename_upload_file, columns)
        self.df_download, self.hash_download = self.read_csv_pandas(self.df_download, basename_download_file, columns)
        self.is_equal_hash(base_name_upload_file, base_name_download_file)
        self.compare_csv()
        return self.df_upload, self.df_download, self.hash_upload, self.hash_download


if __name__ == "__main__":
    zip_ = Zip(sys.argv[1])
    upload_file, download_file = zip_.unzip_file()
    base_name_upload_file: str = os.path.basename(upload_file)
    base_name_download_file: str = os.path.basename(download_file)
    logger.info(f"Upload file: {base_name_upload_file}, Download file: {base_name_download_file}")
    csv_ = Csv(upload_file, download_file)
    df_upload, df_download, hash_upload, hash_download = csv_.main(base_name_upload_file, base_name_download_file)
    zip_.save_files_for_zip(df_upload, df_download, upload_file, download_file, hash_upload, hash_download)
    logger.info(f"{base_name_upload_file, base_name_download_file}: Saved files for zip")
    file_zip = zip_.zip_files(upload_file, download_file)
    logger.info(f"{base_name_upload_file, base_name_download_file}: Moved files to zip {os.path.basename(file_zip)}")
    for files in [f"{upload_file}*", f"{download_file}*"]:
        [os.remove(file) for file in glob.glob(files)]
