#!/bin/bash

xls_path="${XL_IDP_PATH_FTS}/flat_fts/compare_csv"

csv_path="${xls_path}"/csv
if [ ! -d "$csv_path" ]; then
  mkdir "${csv_path}"
fi

done_path="${xls_path}"/done
if [ ! -d "$done_path" ]; then
  mkdir "${done_path}"
fi

find "${xls_path}" -maxdepth 1 -type f \( -name "*.zip" \) ! -newermt '3 seconds ago' -print0 | while read -d $'\0' file
do

  if [[ "${file}" == *"error_"* ]];
  then
    continue
  fi

  mime_type=$(file -b --mime-type "$file")
  echo "'${file} - ${mime_type}'"

  if [[ ${mime_type} = "application/zip" ]]
  then
    python3 ${XL_IDP_ROOT_FTS}/scripts_for_bash_with_inheritance/comparison.py "${file}"
  else
    echo "ERROR: unsupported format ${mime_type}"
    mv "${file}" "${xls_path}/error_$(basename "${file}")"
    continue
  fi

  if [ $? -eq 0 ]
	then
	  mv "${file}" "${done_path}"
	else
	  echo "ERROR during convertion ${file} to csv!"
	  mv "${file}" "${xls_path}/error_$(basename "${file}")"
	  continue
	fi

done