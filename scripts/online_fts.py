import re
import os
import sys
from flat_fts import FTS
from __init__online_fts import *


class OnlineFTS(FTS):
    @staticmethod
    def has_cyrillic(columns):
        list_is_ru_columns = [bool(re.search('[а-яА-Я]', column)) for column in columns]
        return any(list_is_ru_columns)


if __name__ == "__main__":
    online_fts: OnlineFTS = OnlineFTS(os.path.abspath(sys.argv[1]), sys.argv[2], HEADERS_ENG,
                                      LIST_OF_FLOAT_TYPE, LIST_OF_STR_TYPE, LIST_OF_INT_TYPE,
                                      LIST_OF_DATE_TYPE)
    online_fts.main()
