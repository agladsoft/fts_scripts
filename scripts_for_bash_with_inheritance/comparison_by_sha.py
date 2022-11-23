import os
import sys
import zipfile
import hashlib
import contextlib
import numpy as np
import pandas as pd
from pandas import DataFrame
from typing import Tuple, List
from __init__ import headers_eng, logger


def change_types_columns_and_replace_quot_marks(parsed_data: List[dict]) -> None:
    for dict_data in parsed_data:
        for key, value in dict_data.items():
            with contextlib.suppress(Exception):
                dict_data[key] = value.replace('"', '')
                if key in ['the_total_customs_value_of_the_gtd', 'total_invoice_value_for_gtd',
                           'currency_exchange_rate', 'number_of_goods_in_additional_units',
                           'the_number_of_goods_in_the_second_unit_change', 'net_weight_kg', 'gross_weight_kg',
                           'invoice_value', 'customs_value_rub', 'statistical_cost_usd', 'usd_for_kg', 'quota']:
                    dict_data[key] = str(float(value))


def read_csv_pandas(csv_file: str, is_download: bool = False) -> Tuple[DataFrame, str]:
    df = pd.read_csv(csv_file, low_memory=False, dtype=str)
    if is_download:
        df = df.loc[:, ~df.columns.isin(["id", "original_file_name", "original_file_parsed_on"])]
    df = df.rename(columns=headers_eng)
    df.replace({np.NAN: None}, inplace=True)
    parsed_data = df.to_dict('records')
    change_types_columns_and_replace_quot_marks(parsed_data)
    df = pd.DataFrame(parsed_data)
    return df, hashlib.sha256(df.to_string().encode('utf-8')).hexdigest()


def unzip_file(zip_file):
    with zipfile.ZipFile(zip_file, "r") as zf:
        return [zf.extract(name, f"{os.path.dirname(sys.argv[1])}/csv") for name in zf.namelist()]


if __name__ == "__main__":
    upload_file, download_file = unzip_file(sys.argv[1])
    df_upload, hashlib_upload = read_csv_pandas(upload_file)
    df_download, hashlib_download = read_csv_pandas(download_file, True)
    if hashlib_upload == hashlib_download:
        logger.info("Хэш файлов одинаковый")
    else:
        df_upload['flag'] = 'old'
        df_download['flag'] = 'new'
        df_concat = pd.concat([df_upload, df_download])
        duplicates_dropped = df_concat.drop_duplicates(df_concat.columns.difference(['flag']), keep=False)
        duplicates_dropped.to_csv(f'{os.path.dirname(sys.argv[1])}/csv/{os.path.basename(sys.argv[1])}_difference.csv',
                                  index=False)
