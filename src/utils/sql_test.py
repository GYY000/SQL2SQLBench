# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: sql_test.py$
# @Author: 10379
# @Time: 2025/5/17 16:27
import json
import os

import isodate
from tqdm import tqdm

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree
from db_builder.schema_builder import drop_schema, build_db
from utils.db_connector import *
from utils.tools import get_data_path

# print(str(res2[0][1].type._get_full_name()))
print(isodate.parse_duration('DDDDP1Y2M3DT4H5M6S'))
