#coding:utf-8
import ctypes
  
class OgQueryParseResult(ctypes.Structure):
    _fields_ = [
        ("parse_tree_json", ctypes.c_char_p),
        ("err_msg", ctypes.c_char_p),
        ("is_passed", ctypes.c_bool)
    ]

try:
    libpg_query = ctypes.CDLL('./libog_query.so')
    libpg_query.raw_parser_opengauss_dolphin.restype = OgQueryParseResult
    libpg_query.raw_parser_opengauss_dolphin.argtypes = [ctypes.c_char_p]

    sql = b"""
alter table t2 change column b  a varchar;
alter table t3 rename to t4;
drop tables t1,t2,t3;
TRUNCATE TABLE tpcds.reason_t1;
    """

    result = libpg_query.raw_parser_opengauss_dolphin(sql)
    print(sql)
    print(result.parse_tree_json)
    print(result.err_msg)
    print(result.is_passed)

except Exception as e:
    print("Exception", e)

finally:
    if 'result' in locals():
        libpg_query.free_parse_result(ctypes.byref(result))
        print("Memory released successfully")