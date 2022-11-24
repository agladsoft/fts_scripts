#!/bin/bash

xls_path="${XL_IDP_PATH_FTS}/flat_fts/done"

cd "${xls_path}" || exit

find "${xls_path}" -maxdepth 1 -type f \( -name "*.csv" \) ! -newermt '3 seconds ago' -print0 | while read -d $'\0' file
do
  echo "${file}"
  basename_csv_file="$(basename "${file}")"

  psql -d "${POSTGRES_DB}" -U "${POSTGRES_USER}" -c "COPY ( SELECT * FROM fts WHERE original_file_name = '${basename_csv_file}' ORDER BY original_file_index ) TO STDOUT WITH CSV HEADER " > "${xls_path}/down_${basename_csv_file}"
  cp "${basename_csv_file}" "up_${basename_csv_file}"
  zip -r "${file}.zip" "up_${basename_csv_file}" "down_${basename_csv_file}"
  mv "${file}.zip" "${XL_IDP_PATH_FTS}/flat_fts/compare_csv"
  rm -r "up_${basename_csv_file}" "down_${basename_csv_file}"
done