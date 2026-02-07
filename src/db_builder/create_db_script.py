# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: build_db_script$
# @Author: 10379
# @Time: 2025/6/17 21:21
from db_builder.schema_builder import build_test_db, build_db
from utils.tools import get_db_ids, get_all_dialects

for dialect in get_all_dialects():
    for db_id in get_db_ids():
        build_db(db_id, dialect, build_fk=False, all_flag=True, build_idx=True)
        build_test_db(db_id, dialect, all_flag=True)
