import os
import sys
from flat_fts import FTS
from __init__kz_mir import *


class KzMir(FTS):
    pass


if __name__ == "__main__":
    kz_mir: KzMir = KzMir(os.path.abspath(sys.argv[1]), sys.argv[2], HEADERS_ENG,
                          LIST_OF_FLOAT_TYPE, LIST_OF_STR_TYPE, LIST_OF_INT_TYPE,
                          LIST_OF_DATE_TYPE)
    kz_mir.main()
