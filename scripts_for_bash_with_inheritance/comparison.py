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


def compare_csv(df_up: DataFrame, df_down: DataFrame) -> None:
    df_up['flag'] = 'old'
    df_down['flag'] = 'new'
    df_concat: DataFrame = pd.concat([df_up, df_down])
    duplicates_dropped: DataFrame = df_concat.drop_duplicates(df_concat.columns.difference(['flag']), keep=False)
    duplicates_dropped.to_csv(f'{os.path.dirname(sys.argv[1])}/csv/{os.path.basename(sys.argv[1])}_difference.csv',
                              index=False)


def rename_columns(df: DataFrame) -> None:
    dict_columns_eng = {}
    for column, columns in itertools.product(df.columns, headers_eng):
        for column_eng in columns:
            if column == column_eng:
                dict_columns_eng[column] = headers_eng[columns]
    df.rename(columns=dict_columns_eng, inplace=True)


def is_equal_hash(hash_up: str, hash_down: str) -> None:
    if hash_up == hash_down:
        logger.info(f"EQUAL: The hashes of {base_name_upload_file} and {base_name_download_file} are equal")
    else:
        logger.info(f"NOT EQUAL: The hashes of {base_name_upload_file} and {base_name_download_file} are not equal")


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


def read_csv_pandas(df: DataFrame, base_name_csv_file: str, same_columns) -> Tuple[DataFrame, str]:
    df: DataFrame = df.loc[:, df.columns.isin(same_columns)]
    df = df[same_columns]
    df.replace({np.NAN: None}, inplace=True)
    parsed_data: List[dict] = df.to_dict('records')
    logger.info(f"{base_name_csv_file}: Dataframe have been converted to a list with dictionary")
    change_types_columns_and_replace_quot_marks(parsed_data)
    logger.info(f"{base_name_csv_file}: Changed column types")
    df = pd.DataFrame(parsed_data)
    logger.info(f"{base_name_csv_file}: List with dictionary converted to a dataframe")
    hash_file: str = hashlib.md5(str(parsed_data).encode()).hexdigest()
    logger.info(f"{base_name_csv_file}: Hash is {hash_file}")
    return df, hash_file


def unzip_file(zip_file: str) -> List[str]:
    with zipfile.ZipFile(zip_file, "r") as zf:
        return [zf.extract(name, f"{os.path.dirname(sys.argv[1])}/csv") for name in sorted(zf.namelist(), reverse=True)]


def save_files_for_zip(df_up: DataFrame, df_down: DataFrame, up_file: str, down_file: str, hash_up: str,
                       hash_down: str) -> None:
    with open(f"{up_file}.txt", "w") as hash_up_file:
        hash_up_file.write(hash_up)
    with open(f"{down_file}.txt", "w") as hash_down_file:
        hash_down_file.write(hash_down)
    df_up.to_csv(up_file, index=False)
    df_down.to_csv(down_file, index=False)


def zip_files(zip_file: str, up_file: str, down_file: str) -> str:
    absolute_file_zip = f"{os.path.dirname(zip_file)}/done/{os.path.basename(zip_file)}_compared.zip"
    with zipfile.ZipFile(absolute_file_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(up_file, os.path.basename(up_file))
        zf.write(down_file, os.path.basename(down_file))
        zf.write(f"{up_file}.txt", os.path.basename(f"{up_file}.txt"))
        zf.write(f"{down_file}.txt", os.path.basename(f"{down_file}.txt"))
    return absolute_file_zip

def read_same_columns_from_csv(up_file: str, down_file: str) -> Tuple[DataFrame, DataFrame, list]:
    df_up: DataFrame = pd.read_csv(up_file, low_memory=False, dtype=str)
    df_down: DataFrame = pd.read_csv(down_file, low_memory=False, dtype=str)
    rename_columns(df_up)
    same_columns = list(set(list(df_down.columns)).intersection(set(list(df_up.columns))))
    return df_up, df_down, same_columns

if __name__ == "__main__":
    upload_file, download_file = unzip_file(sys.argv[1])
    base_name_upload_file: str = os.path.basename(upload_file)
    base_name_download_file: str = os.path.basename(download_file)
    logger.info(f"Upload file: {base_name_upload_file}, Download file: {base_name_download_file}")
    df_upload, df_download, columns = read_same_columns_from_csv(upload_file, download_file)
    df_upload, hash_upload = read_csv_pandas(df_upload, base_name_upload_file, columns)
    df_download, hash_download = read_csv_pandas(df_download, base_name_download_file, columns)
    is_equal_hash(hash_upload, hash_download)
    compare_csv(df_upload, df_download)
    save_files_for_zip(df_upload, df_download, upload_file, download_file, hash_upload, hash_download)
    logger.info(f"{base_name_upload_file, base_name_download_file}: Saved files for zip")
    file_zip = zip_files(sys.argv[1], upload_file, download_file)
    logger.info(f"{base_name_upload_file, base_name_download_file}: Moved files to zip {os.path.basename(file_zip)}")
    for files in [f"{upload_file}*", f"{download_file}*"]:
        [os.remove(file) for file in glob.glob(files)]
