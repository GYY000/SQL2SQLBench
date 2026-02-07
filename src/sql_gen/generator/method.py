# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: method$
# @Author: 10379
# @Time: 2025/5/10 12:24
import copy
import json
import os
import random
import re
from typing import Optional

from antlr_parser.Tree import TreeNode
from antlr_parser.general_tree_analysis import inside_aggregate_function, fetch_query_body_node, fetch_all_ctes, \
    build_ctes, dfs_table_ref_node
from antlr_parser.mysql_tree import fetch_all_simple_select_from_select_stmt_mysql
from antlr_parser.oracle_tree import fetch_all_simple_select_from_subquery_oracle
from antlr_parser.parse_tree import parse_tree
from antlr_parser.pg_tree import fetch_all_simple_select_from_select_stmt_pg, get_pg_main_select_node_from_select_stmt
from sql_gen.generator.MAGIC_TOKENS import AGGREGATE_FUNCTION_TOKEN
from sql_gen.generator.ele_type.operand_analysis import analysis_ctes
from sql_gen.generator.ele_type.type_conversion import type_mapping
from sql_gen.generator.ele_type.type_def import BaseType, is_num_type, is_str_type, is_time_type, IntGeneralType, \
    NullType
from sql_gen.generator.element.Operand import Operand, ColumnOp
from sql_gen.generator.element.Pattern import ForSlot
from utils.ExecutionEnv import ExecutionEnv
from utils.tools import get_db_ids, get_proj_root_path, get_used_reserved_keyword_list, get_table_col_name, \
    get_schema_path, is_any_reserved_keyword


def fetch_fulfilled_sqls(points: list[dict], src_dialect, tgt_dialect, db_id: str = None):
    if db_id is not None:
        db_ids = [db_id]
    else:
        db_ids = get_db_ids()
    all_sqls = []
    for db in db_ids:
        sql_root_path = os.path.join(get_proj_root_path(), 'SQL', db)
        if os.path.exists(os.path.join(sql_root_path, 'no_points')):
            path1 = os.path.join(sql_root_path, 'no_points', f'{src_dialect}_{tgt_dialect}.json')
            path2 = os.path.join(sql_root_path, 'no_points', f'{tgt_dialect}_{src_dialect}.json')
            if os.path.exists(path1):
                with open(path1, 'r') as file:
                    sqls = json.load(file)
            elif os.path.exists(path2):
                with open(path2, 'r') as file:
                    sqls = json.load(file)
            else:
                assert False
            assert sqls is not None
            for sql in sqls:
                flag = True
                if 'points' in sql:
                    if len(sql['points']) > 0:
                        print(path1)
                        print(path2)
                        print(sql)
                point_list = sql.get('points', [])
                for point in point_list:
                    if point not in points:
                        flag = False
                        break
                if flag:
                    all_sqls.append(sql)
        path = os.path.join(sql_root_path, 'points', f'{src_dialect}_{tgt_dialect}.json')
        if os.path.exists(path):
            with open(path, 'r') as file:
                sqls = json.load(file)
            for sql in sqls:
                flag = True
                for point in sql['points']:
                    if point not in points:
                        flag = False
                        break
                if flag:
                    all_sqls.append(sql)
    for sql in all_sqls:
        if 'points' not in sql:
            sql['points'] = []
    random.shuffle(all_sqls)
    return all_sqls


def mark_in_aggregate_slot(node: TreeNode, dialect: str, aggregate_slot_set: set):
    if node.slot is not None:
        if inside_aggregate_function(dialect, node):
            aggregate_slot_set.add(node.slot)
        return
    if len(node.for_loop_sub_trees) > 0:
        for i in range(len(node.for_loop_sub_trees)):
            sub_tree = node.for_loop_sub_trees[i]
            first_tree = sub_tree['first_tree']
            assert isinstance(first_tree, TreeNode)
            first_tree.father = node.father
            new_set = set()
            for child in first_tree.children:
                mark_in_aggregate_slot(child, dialect, new_set)
            for_loop = node.for_loop_slot[i]
            assert isinstance(for_loop, ForSlot)
            for j in range(len(for_loop.sub_ele_slots)):
                if for_loop.sub_ele_slots[j] in new_set:
                    aggregate_slot_set.add(for_loop.ele_slots[j])
        return
    for child in node.children:
        assert isinstance(child, TreeNode)
        mark_in_aggregate_slot(child, dialect, aggregate_slot_set)


def merge_trans_points(points1: list[dict], points2: list[dict]):
    res = copy.deepcopy(points1)
    for point in points2:
        flag = False
        for exist_point in res:
            if exist_point['point'] == point['point']:
                exist_point['num'] = exist_point['num'] + point['num']
                flag = True
        if not flag:
            res.append(point)
    return res


def add_point_to_point_dict(points1: list[dict], point):
    flag = False
    if isinstance(point, str):
        point_name = point
    elif isinstance(point, dict):
        point_name = point['Desc']
    else:
        point_name = point.point_name
    for exist_point in points1:
        if exist_point['point'] == point_name:
            exist_point['num'] = exist_point['num'] + 1
            flag = True
    if not flag:
        points1.append({
            "point": point_name,
            "num": 1
        })


def has_limit_order_by(select_body_node: TreeNode, dialect: str):
    if dialect == 'mysql':
        if select_body_node.get_child_by_value('orderByClause') is not None or select_body_node.get_child_by_value(
                'limitClause') is not None:
            return True
        if len(select_body_node.get_children_by_value('unionStatement')) == 0:
            # only one selectStatement
            if select_body_node.get_child_by_value('querySpecification') is not None:
                node = select_body_node.get_child_by_value('querySpecification')
            elif select_body_node.get_child_by_value('querySpecificationNointo') is not None:
                node = select_body_node.get_child_by_value('querySpecificationNointo')
            else:
                node = None
            if isinstance(node, TreeNode) and node.get_child_by_value(
                    'orderByClause') is not None or node.get_child_by_value('limitClause') is not None:
                return True
        else:
            union_statement_node = select_body_node.get_children_by_value('unionStatement')[-1]
            if union_statement_node.get_child_by_value('querySpecificationNointo') is not None:
                node = select_body_node.get_child_by_value('querySpecificationNointo')
                if isinstance(node, TreeNode) and node.get_child_by_value(
                        'orderByClause') is not None or node.get_child_by_value('limitClause') is not None:
                    return True
    elif dialect == 'pg':
        # select_no_parens
        if (select_body_node.get_child_by_value('opt_sort_clause') is not None or
                select_body_node.get_child_by_value('opt_select_limit') is not None or
                select_body_node.get_child_by_value(
                    'select_limit') is not None):
            return True
    elif dialect == 'oracle':
        if (select_body_node.get_child_by_value('order_by_clause') is not None or
                select_body_node.get_child_by_value('offset_clause') is not None):
            return True
        main_body_node = select_body_node.get_children_by_path(['select_only_statement', 'subquery'])
        assert len(main_body_node) == 1
        main_body_node = main_body_node[0]
        if len(main_body_node.get_children_by_value('subquery_operation_part')) == 0:
            if main_body_node.get_child_by_value('subquery_basic_elements') is not None:
                node = main_body_node.get_child_by_value('subquery_basic_elements')
                if node.get_child_by_value('query_block') is not None:
                    node = node.get_child_by_value('query_block')
                    if (node.get_child_by_value('order_by_clause') is not None or
                            node.get_child_by_value('offset_clause') is not None):
                        return True
        else:
            union_statement_node = main_body_node.get_children_by_value('subquery_operation_part')[-1]
            if union_statement_node.get_child_by_value('subquery_basic_elements') is not None:
                node = union_statement_node.get_child_by_value('subquery_basic_elements')
                if node.get_child_by_value('query_block') is not None:
                    node = node.get_child_by_value('query_block')
                    if (node.get_child_by_value('order_by_clause') is not None or
                            node.get_child_by_value('offset_clause') is not None):
                        return True
    else:
        assert False
    return False


def rm_outer_limit_order_by(select_body_node: TreeNode, dialect: str):
    pass


def fetch_all_select_stmts(root_node: TreeNode, dialect: str):
    if dialect == 'pg':
        select_stmt_node = root_node.get_children_by_path(['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        simple_select_nodes = fetch_all_simple_select_from_select_stmt_pg(select_stmt_node)
    elif dialect == 'mysql':
        select_statement_node = root_node.get_children_by_path(['sqlStatements', 'sqlStatement',
                                                                'dmlStatement', 'selectStatement'])
        assert len(select_statement_node) == 1
        select_stmt_node = select_statement_node[0]
        simple_select_nodes = fetch_all_simple_select_from_select_stmt_mysql(select_stmt_node)
    elif dialect == 'oracle':
        subquery_node = root_node.get_children_by_path(['unit_statement', 'data_manipulation_language_statements',
                                                        'select_statement', 'select_only_statement', 'subquery'])
        if len(subquery_node) != 1:
            print('FOR UPDATE haven\'t been supported yet')
            assert False
        select_stmt_node = subquery_node[0]
        simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(select_stmt_node)
    else:
        assert False
    return simple_select_nodes


def rm_select_node(select_stmts: list[TreeNode], src_dialect: str, index_i):
    for select_stmt in select_stmts:
        if src_dialect == 'mysql':
            selectElements_node = select_stmt.get_child_by_value('selectElements')
            selectElement_nodes = selectElements_node.get_children_by_value('selectElement')
            if len(selectElement_nodes) == 0:
                return
            to_rm_node = selectElement_nodes[index_i]
            i = 0
            for child in selectElements_node.children:
                if to_rm_node == child:
                    if i >= 1 and selectElements_node.children[i - 1].value == ',':
                        selectElements_node.rm_child(selectElements_node.children[i - 1])
                    selectElements_node.rm_child(to_rm_node)
                    break
                i = i + 1
        elif src_dialect == 'pg':
            if select_stmt.get_child_by_value('opt_target_list') is not None:
                opt_target_list = select_stmt.get_child_by_value('opt_target_list')
                target_list_node = opt_target_list.get_child_by_value('target_list')
            else:
                target_list_node = select_stmt.get_child_by_value('target_list')
            selectElement_nodes = target_list_node.get_children_by_value('target_el')
            if len(selectElement_nodes) == 0:
                return
            to_rm_node = selectElement_nodes[index_i]
            i = 0
            for child in target_list_node.children:
                if to_rm_node == child:
                    if i >= 1 and target_list_node.children[i - 1].value == ',':
                        target_list_node.rm_child(target_list_node.children[i - 1])
                    assert isinstance(target_list_node, TreeNode)
                    target_list_node.rm_child(to_rm_node)
                    break
                i = i + 1
        elif src_dialect == 'oracle':
            selectElements_node = select_stmt.get_child_by_value('selected_list')
            selectElement_nodes = selectElements_node.get_children_by_value('select_list_elements')
            if len(selectElement_nodes) == 0:
                return
            to_rm_node = selectElement_nodes[index_i]
            i = 0
            for child in selectElements_node.children:
                if to_rm_node == child:
                    if i >= 1 and selectElements_node.children[i - 1].value == ',':
                        selectElements_node.rm_child(selectElements_node.children[i - 1])
                    selectElements_node.rm_child(to_rm_node)
                    break
                i = i + 1
        else:
            assert False


def add_null_node(select_stmts: list[TreeNode], src_dialect: str, ori_type, use_null_flag: bool = False):
    if is_num_type(ori_type):
        if use_null_flag:
            null_node = TreeNode('CAST(NULL AS REAL)', src_dialect, True)
        else:
            null_node = TreeNode('0.0', src_dialect, True)
    elif is_time_type(ori_type):
        if use_null_flag:
            null_node = TreeNode('CAST(null AS DATE)', src_dialect, True)
        else:
            null_node = TreeNode('DATE \'2025-05-30\'', src_dialect, True)
    else:
        null_node = TreeNode('NULL', src_dialect, True)
    for select_stmt in select_stmts:
        if src_dialect == 'mysql':
            selectElements_node = select_stmt.get_child_by_value('selectElements')
            selectElement_nodes = selectElements_node.get_children_by_value('selectElement')
            if len(selectElement_nodes) == 0:
                return
            new_mark_node = TreeNode(',', src_dialect, True)
            selectElements_node.add_child(new_mark_node)
            new_select_element_node = TreeNode('selectElement', src_dialect, False)
            new_select_element_node.add_child(null_node)
            selectElements_node.add_child(new_select_element_node)
        elif src_dialect == 'pg':
            if select_stmt.get_child_by_value('opt_target_list') is not None:
                opt_target_list = select_stmt.get_child_by_value('opt_target_list')
                target_list_node = opt_target_list.get_child_by_value('target_list')
            else:
                target_list_node = select_stmt.get_child_by_value('target_list')
            selectElement_nodes = target_list_node.get_children_by_value('target_el')
            if len(selectElement_nodes) == 0:
                return
            new_mark_node = TreeNode(',', src_dialect, True)
            target_list_node.add_child(new_mark_node)
            new_select_element_node = TreeNode('target_el', src_dialect, False)
            new_select_element_node.add_child(null_node)
            target_list_node.add_child(new_select_element_node)
        elif src_dialect == 'oracle':
            selectElements_node = select_stmt.get_child_by_value('selected_list')
            selectElement_nodes = selectElements_node.get_children_by_value('select_list_elements')
            if len(selectElement_nodes) == 0:
                return
            new_mark_node = TreeNode(',', src_dialect, True)
            selectElements_node.add_child(new_mark_node)
            new_select_element_node = TreeNode('select_list_elements', src_dialect, False)
            new_select_element_node.add_child(null_node)
            selectElements_node.add_child(new_select_element_node)
        else:
            assert False


def reorder_col(select_stmts: list[TreeNode], src_dialect: str, index_i, index_j):
    for select_stmt in select_stmts:
        if src_dialect == 'mysql':
            selectElements_node = select_stmt.get_child_by_value('selectElements')
            selectElement_nodes = selectElements_node.get_children_by_value('selectElement')
            if len(selectElement_nodes) == 0:
                return False
            child_i = selectElement_nodes[index_i]
            child_j = selectElement_nodes[index_j]
            for i in range(len(selectElements_node.children)):
                if selectElements_node.children[i] == child_i:
                    selectElements_node.children[i] = child_j
                elif selectElements_node.children[i] == child_j:
                    selectElements_node.children[i] = child_i
        elif src_dialect == 'pg':
            if select_stmt.get_child_by_value('opt_target_list') is not None:
                opt_target_list = select_stmt.get_child_by_value('opt_target_list')
                target_list_node = opt_target_list.get_child_by_value('target_list')
            else:
                target_list_node = select_stmt.get_child_by_value('target_list')
            selectElement_nodes = target_list_node.get_children_by_value('target_el')
            if len(selectElement_nodes) == 0:
                return False
            child_i = selectElement_nodes[index_i]
            child_j = selectElement_nodes[index_j]
            for i in range(len(target_list_node.children)):
                if target_list_node.children[i] == child_i:
                    target_list_node.children[i] = child_j
                elif target_list_node.children[i] == child_j:
                    target_list_node.children[i] = child_i
        elif src_dialect == 'oracle':
            selectElements_node = select_stmt.get_child_by_value('selected_list')
            selectElement_nodes = selectElements_node.get_children_by_value('select_list_elements')
            if len(selectElement_nodes) == 0:
                return False
            child_i = selectElement_nodes[index_i]
            child_j = selectElement_nodes[index_j]
            for i in range(len(selectElements_node.children)):
                if selectElements_node.children[i] == child_i:
                    selectElements_node.children[i] = child_j
                elif selectElements_node.children[i] == child_j:
                    selectElements_node.children[i] = child_i
        else:
            assert False
    return True


def add_col_null(select_stmts: list[TreeNode], src_dialect: str, index_i: int, ori_type, null_mode=False):
    if isinstance(ori_type, IntGeneralType):
        null_node = TreeNode('0', src_dialect, True)
    elif is_num_type(ori_type):
        if null_mode:
            null_node = TreeNode('CAST(NULL AS REAL)', src_dialect, True)
        else:
            null_node = TreeNode('0.0', src_dialect, True)
    else:
        null_node = TreeNode('NULL', src_dialect, True)
    if src_dialect == 'mysql':
        father_node = TreeNode('selectElement', src_dialect, False)
    elif src_dialect == 'pg':
        father_node = TreeNode('target_el', src_dialect, False)
    elif src_dialect == 'oracle':
        father_node = TreeNode('select_list_elements', src_dialect, False)
    else:
        assert False
    father_node.add_child(null_node)
    for select_stmt in select_stmts:
        if src_dialect == 'mysql':
            selectElements_node = select_stmt.get_child_by_value('selectElements')
            selectElement_nodes = selectElements_node.get_children_by_value('selectElement')
            if len(selectElement_nodes) == 0:
                return None
            child_i = selectElement_nodes[index_i]
            insert_idx = None
            for i, child in enumerate(selectElements_node.children):
                if child == child_i:
                    insert_idx = i
            selectElements_node.children.insert(insert_idx, father_node)
            new_mark_node = TreeNode(',', src_dialect, True)
            selectElements_node.children.insert(insert_idx + 1, new_mark_node)
        elif src_dialect == 'pg':
            if select_stmt.get_child_by_value('opt_target_list') is not None:
                opt_target_list = select_stmt.get_child_by_value('opt_target_list')
                target_list_node = opt_target_list.get_child_by_value('target_list')
            else:
                target_list_node = select_stmt.get_child_by_value('target_list')
            selectElement_nodes = target_list_node.get_children_by_value('target_el')
            if len(selectElement_nodes) == 0:
                return False
            child_i = selectElement_nodes[index_i]
            insert_idx = None
            for i, child in enumerate(target_list_node.children):
                if child == child_i:
                    insert_idx = i
            target_list_node.children.insert(insert_idx, father_node)
            new_mark_node = TreeNode(',', src_dialect, True)
            target_list_node.children.insert(insert_idx + 1, new_mark_node)
        elif src_dialect == 'oracle':
            selectElements_node = select_stmt.get_child_by_value('selected_list')
            selectElement_nodes = selectElements_node.get_children_by_value('select_list_elements')
            if len(selectElement_nodes) == 0:
                return False
            child_i = selectElement_nodes[index_i]
            insert_idx = None
            for i, child in enumerate(selectElements_node.children):
                if child == child_i:
                    insert_idx = i
            selectElements_node.children.insert(insert_idx, father_node)
            new_mark_node = TreeNode(',', src_dialect, True)
            selectElements_node.children.insert(insert_idx + 1, new_mark_node)
        else:
            assert False
    return True


def rep_select_stmt_with_cte(root_node: TreeNode, select_stmt_nodes: list[TreeNode], src_dialect):
    flag, ctes = fetch_all_ctes(root_node, src_dialect)
    if not flag:
        return
    rep_node_dict = {}
    for index, select_stmt_node in enumerate(select_stmt_nodes):
        select_stmt_sql = str(select_stmt_node)
        pattern = r"^SELECT\s+\*\s+FROM\s+((`[^`]*`)|(\"[^\"]*\")|(\[[^\]]*\])|([a-zA-Z_][a-zA-Z0-9_]*))\s*;?\s*$"
        match = re.match(pattern, select_stmt_sql, re.IGNORECASE)
        if not match:
            continue
        full_match = match.group(1)
        if full_match.startswith('`') and full_match.endswith('`'):
            to_match_table_name = full_match[1:-1]
        elif full_match.startswith('"') and full_match.endswith('"'):
            to_match_table_name = full_match[1:-1]
        elif full_match.startswith('[') and full_match.endswith(']'):
            to_match_table_name = full_match[1:-1]
        else:
            to_match_table_name = full_match
        for cte in ctes['cte_list']:
            if cte['cte_name'].lower() == to_match_table_name.lower():
                rep_node_dict[index] = cte['query']
                break
    for key, value in rep_node_dict.items():
        select_stmt_nodes[index] = value


def process_to_add_sql(sql1_types: list[BaseType] | None, sql1: str | TreeNode, sql2: str | TreeNode, src_dialect: str,
                       execute_env: ExecutionEnv, del_mode: bool, use_null_flag: bool = False):
    if sql1_types is None:
        flag, res = execute_env.fetch_type(sql1, False)
        if not flag:
            return None, None
        sql1_types = [type_mapping(src_dialect, t['type']) for t in res]

    if isinstance(sql1, str):
        sql1_root_node, _, _, _ = parse_tree(sql1, src_dialect)
        if sql1_root_node is None:
            return None
        sql1_root_node = TreeNode.make_g4_tree_by_node(sql1_root_node, src_dialect)
    else:
        sql1_root_node = sql1
    if isinstance(sql2, str):
        sql2_root_node, _, _, _ = parse_tree(sql2, src_dialect)
        if sql2_root_node is None:
            return None
        sql2_root_node = TreeNode.make_g4_tree_by_node(sql2_root_node, src_dialect)
    else:
        sql2_root_node = sql2

    flag, res = execute_env.fetch_type(sql2, False)
    if not flag:
        print(sql2)
    sql2_types = [type_mapping(src_dialect, t['type']) for t in res]
    i = 0
    query1_select_stmts = fetch_all_select_stmts(sql1_root_node, src_dialect)
    rep_select_stmt_with_cte(sql1_root_node, query1_select_stmts, src_dialect)
    query2_select_stmts = fetch_all_select_stmts(sql2_root_node, src_dialect)
    rep_select_stmt_with_cte(sql2_root_node, query2_select_stmts, src_dialect)
    for sql1_type in sql1_types:
        flag = False
        if i >= len(sql2_types):
            break
        if is_num_type(sql1_type):
            for j in range(len(sql2_types)):
                if j < i:
                    continue
                if is_num_type(sql2_types[j]):
                    reorder_col(query2_select_stmts, src_dialect, i, j)
                    sql2_types[j], sql2_types[i] = sql2_types[i], sql2_types[j]
                    flag = True
        if is_str_type(sql1_type):
            for j in range(len(sql2_types)):
                if j < i:
                    continue
                if is_str_type(sql2_types[j]):
                    reorder_col(query2_select_stmts, src_dialect, i, j)
                    sql2_types[j], sql2_types[i] = sql2_types[i], sql2_types[j]
                    flag = True
        if is_time_type(sql1_type):
            for j in range(len(sql2_types)):
                if j < i:
                    continue
                if is_time_type(sql2_types[j]):
                    reorder_col(query2_select_stmts, src_dialect, i, j)
                    sql2_types[j], sql2_types[i] = sql2_types[i], sql2_types[j]
                    flag = True
        if not flag:
            add_col_null(query2_select_stmts, src_dialect, i, sql1_type)
            sql2_types.insert(i, NullType())
        i += 1
    if len(sql1_types) < len(sql2_types):
        sql1_types_len = len(sql1_types)
        j = len(sql2_types) - 1
        t = 0
        while j >= len(sql1_types):
            if del_mode:
                rm_select_node(query2_select_stmts, src_dialect, j)
            else:
                add_null_node(query1_select_stmts, src_dialect, sql2_types[sql1_types_len + t], use_null_flag)
            j -= 1
            t += 1
    if len(sql1_types) > len(sql2_types):
        j = len(sql1_types) - 1
        t = 0
        sql2_types_len = len(sql2_types)
        while j >= len(sql2_types):
            add_null_node(query2_select_stmts, src_dialect, sql1_types[sql2_types_len + t], use_null_flag)
            j -= 1
            t += 1
    return str(sql1_root_node), str(sql2_root_node)


def fetch_used_alias(from_clause_node, dialect, cte_names):
    used_subquery_alias = set()
    table_table_used_name_map = {}
    table_table_node_map = {}
    if dialect == 'mysql':
        if from_clause_node is not None:
            table_source_nodes = from_clause_node.get_children_by_path(['tableSources', 'tableSource'])
            table_source_items = []
            for table_source_node in table_source_nodes:
                table_source_item_node = table_source_node.get_child_by_value('tableSourceItem')
                table_source_items.append(table_source_item_node)
                assert table_source_item_node is not None
                join_part_nodes = table_source_node.get_children_by_value('joinPart')
                for join_part_node in join_part_nodes:
                    table_source_item_node = join_part_node.get_child_by_value('tableSourceItem')
                    table_source_items.append(table_source_item_node)
            for table_source_item_node in table_source_items:
                if table_source_item_node.get_child_by_value('tableSources') is not None:
                    assert False
                if table_source_item_node.get_child_by_value('dmlStatement') is not None:
                    if table_source_item_node.get_child_by_value('uid') is not None:
                        name = str(table_source_item_node.get_child_by_value('uid')).strip('`')
                        used_subquery_alias.add(name)
                else:
                    assert table_source_item_node.get_child_by_value('tableName') is not None
                    name = str(table_source_item_node.get_child_by_value('tableName'))
                    name = name.strip('`')
                    if table_source_item_node.get_child_by_value('uid') is not None:
                        alias_name = str(table_source_item_node.get_child_by_value('uid')).strip('`')
                    else:
                        alias_name = name
                    used_subquery_alias.add(alias_name)
                    if not name.upper() in cte_names:
                        table_table_used_name_map[name] = alias_name
                        table_table_node_map[name] = table_source_item_node
    elif dialect == 'pg':
        if from_clause_node is not None:
            assert isinstance(from_clause_node, TreeNode)
            table_ref_nodes = from_clause_node.get_children_by_path(['from_list', 'table_ref'])
            all_table_ref_nodes = []
            for table_ref_node in table_ref_nodes:
                all_table_ref_nodes += dfs_table_ref_node(table_ref_node)
            for table_ref_node in all_table_ref_nodes:
                if table_ref_node.get_child_by_value('relation_expr') is not None:
                    name = str(table_ref_node.get_child_by_value('relation_expr')).strip('"')
                    name = name.strip('`')
                    if table_ref_node.get_child_by_value('opt_alias_clause') is not None:
                        alias_name = table_ref_node.get_children_by_path(
                            ['opt_alias_clause', 'table_alias_clause', 'table_alias'])
                        assert len(alias_name) > 0
                        alias_name = str(alias_name[0]).strip('"')
                    else:
                        alias_name = name
                    used_subquery_alias.add(alias_name)
                    if not name.upper() in cte_names:
                        table_table_used_name_map[name] = alias_name
                        table_table_node_map[name] = table_ref_node.get_child_by_value('relation_expr')
                elif table_ref_node.get_child_by_value('select_with_parens') is not None:
                    if table_ref_node.get_child_by_value('opt_alias_clause') is not None:
                        alias_name = table_ref_node.get_children_by_path(
                            ['opt_alias_clause', 'table_alias_clause', 'table_alias'])
                        assert len(alias_name) > 0
                        name = str(alias_name[0]).strip('"')
                        used_subquery_alias.add(name)
    elif dialect == 'oracle':
        if from_clause_node is not None:
            assert isinstance(from_clause_node, TreeNode)
            table_ref_nodes = from_clause_node.get_children_by_path(['table_ref_list', 'table_ref'])
            all_table_ref_aux_nodes = []
            for table_ref_node in table_ref_nodes:
                table_ref_aux_node = table_ref_node.get_child_by_value('table_ref_aux')
                assert table_ref_aux_node is not None
                all_table_ref_aux_nodes.append(table_ref_aux_node)
                for join_clause in table_ref_node.get_children_by_value('join_clause'):
                    table_ref_aux_node = join_clause.get_child_by_value('table_ref_aux')
                    assert table_ref_aux_node is not None
                    all_table_ref_aux_nodes.append(table_ref_aux_node)
            for table_ref_aux_node in all_table_ref_aux_nodes:
                table_ref_aux_internal_node = table_ref_aux_node.get_child_by_value('table_ref_aux_internal')
                assert table_ref_aux_internal_node is not None
                assert table_ref_aux_internal_node.get_child_by_value('table_ref_aux_internal') is None
                dml_table_expression_clause_node = table_ref_aux_internal_node.get_child_by_value(
                    'dml_table_expression_clause')
                if dml_table_expression_clause_node.get_child_by_value('tableview_name') is not None:
                    name = str(dml_table_expression_clause_node.get_child_by_value('tableview_name')).strip('"')
                    alias_node = table_ref_aux_node.get_child_by_value('table_alias')
                    if alias_node is not None:
                        alias_name = str(alias_node).strip('"')
                    else:
                        alias_name = name
                    used_subquery_alias.add(alias_name)
                    if not name.upper() in cte_names:
                        table_table_used_name_map[name] = alias_name
                        table_table_node_map[name] = dml_table_expression_clause_node.get_child_by_value('tableview_name')
                elif dml_table_expression_clause_node.get_child_by_value('select_statement') is not None:
                    alias_node = table_ref_aux_node.get_child_by_value('table_alias')
                    if alias_node is not None:
                        alias_name = str(alias_node).strip('"')
                        used_subquery_alias.add(alias_name)
                elif dml_table_expression_clause_node.get_child_by_value('subquery') is not None:
                    alias_node = table_ref_aux_node.get_child_by_value('table_alias')
                    if alias_node is not None:
                        alias_name = str(alias_node).strip('"')
                        used_subquery_alias.add(alias_name)
                else:
                    continue
    return used_subquery_alias, table_table_used_name_map, table_table_node_map


reserved_keywords_list = get_used_reserved_keyword_list()


def get_used_alias_name(point, existing_alias):
    if isinstance(point, str):
        ori_name = point
    else:
        ori_name = point.point_name
    used_name = ''
    flag = True
    for word in ori_name:
        if not (word.isalnum() or word == ' '):
            flag = False
            break
    if flag:
        for splits in ori_name.split():
            if used_name != '':
                used_name = used_name + '_'
            used_name = used_name + splits
        used_name = used_name + 'COL'
    else:
        used_name = 'COL_ALIAS'
    num = 0
    used_name = used_name[:20]
    if (used_name.upper() in existing_alias
            or used_name.upper() in reserved_keywords_list['mysql'] or used_name.upper() in reserved_keywords_list['pg']
            or used_name.upper() in reserved_keywords_list['oracle']):
        while used_name + f'_{num}' in existing_alias:
            num += 1
        used_name = used_name + f'_{num}'
    return used_name


def add_op_to_select(op: Operand, point, src_dialect: str, tgt_dialect, select_stmt_node: TreeNode,
                     actual_add_flag: bool):
    if not actual_add_flag:
        return
    if src_dialect == 'mysql':
        assert select_stmt_node.value == 'querySpecificationNointo' or select_stmt_node.value == 'querySpecification'
        select_elements_node = select_stmt_node.get_child_by_value('selectElements')
        assert isinstance(select_elements_node, TreeNode)
        alias_list = []
        if ((len(select_elements_node.children) == 1 and str(select_elements_node) == str(AGGREGATE_FUNCTION_TOKEN)) or
                select_elements_node.get_child_by_value('*') is not None):
            select_elements_node.children = []
        else:
            select_element_nodes = select_elements_node.get_children_by_value('selectElement')
            for select_element_node in select_element_nodes:
                alias = None
                if select_element_node.get_child_by_value('uid') is not None:
                    alias = str(select_element_node.get_child_by_value('uid')).strip('`')
                elif select_element_node.get_child_by_value('fullColumnName') is not None:
                    full_column_name_node = select_element_node.get_child_by_value('fullColumnName')
                    if full_column_name_node.get_child_by_value('dottedId') is not None:
                        alias = str(full_column_name_node.get_child_by_value('dottedId')).strip('.').strip('`')
                    else:
                        assert full_column_name_node.get_child_by_value('uid') is not None
                        alias = str(full_column_name_node.get_child_by_value('uid')).strip('`')
                if alias is not None:
                    alias_list.append(alias.upper())
            select_elements_node.add_child(TreeNode(',', src_dialect, True))
        select_element_node = TreeNode('selectElement', src_dialect, False)
        select_element_node.add_child(TreeNode(op.str_value(), src_dialect, True))
        if tgt_dialect != 'oracle':
            select_element_node.add_child(TreeNode('AS', src_dialect, True))
        alias_node = TreeNode('uid', src_dialect, False)
        alias_node.add_child(TreeNode(get_used_alias_name(point, alias_list), src_dialect, True))
        select_element_node.add_child(alias_node)
        select_elements_node.add_child(select_element_node)
    elif src_dialect == 'pg':
        assert select_stmt_node.value == 'simple_select_pramary'
        target_list_node = select_stmt_node.get_child_by_value('target_list')
        if target_list_node is None:
            opt_target_list = select_stmt_node.get_child_by_value('opt_target_list')
            target_list_node = opt_target_list.get_child_by_value('target_list')
        assert target_list_node is not None
        target_els = target_list_node.get_children_by_value('target_el')
        flag = False
        for target_el in target_els:
            if str(target_el) == '*' or str(target_el) == str(AGGREGATE_FUNCTION_TOKEN):
                flag = True
        alis_list = []
        if flag:
            target_list_node.children = []
        else:
            target_list_node.add_child(TreeNode(',', src_dialect, True))
            for target_el in target_els:
                if target_el.get_child_by_value('collabel') is not None:
                    alis_list.append(str(target_el.get_child_by_value('collabel')).strip('"'))
                elif target_el.get_child_by_value('identifier') is not None:
                    alis_list.append(str(target_el.get_child_by_value('identifier')).strip('"'))
                else:
                    alis_list.append(str(target_el.get_child_by_value('a_expr')).strip('"'))
        target_el_node = TreeNode('target_el', src_dialect, False)
        target_el_node.add_child(TreeNode(op.str_value(), src_dialect, True))
        if tgt_dialect != 'oracle':
            target_el_node.add_child(TreeNode('AS', src_dialect, True))
        alias_node = TreeNode('identifier', src_dialect, False)
        alias_node.add_child(TreeNode(get_used_alias_name(point, alis_list), src_dialect, True))
        target_el_node.add_child(alias_node)
        target_list_node.add_child(target_el_node)
    elif src_dialect == 'oracle':
        assert select_stmt_node.value == 'query_block'
        selected_list_node = select_stmt_node.get_child_by_value('selected_list')
        assert selected_list_node is not None
        alias_list = []
        if (selected_list_node.get_child_by_value('*') is not None
                or str(selected_list_node) == str(AGGREGATE_FUNCTION_TOKEN)):
            selected_list_node.children = []
        else:
            for element in selected_list_node.get_children_by_value('select_list_elements'):
                if element.get_child_by_value('column_alias') is not None:
                    column_alias_node = element.get_child_by_value('column_alias')
                    if column_alias_node.get_child_by_value('identifier') is not None:
                        alias_list.append(str(column_alias_node.get_child_by_value('identifier')).strip('"').upper())
                    elif column_alias_node.get_child_by_value('quoted_string') is not None:
                        alias_list.append(
                            str(column_alias_node.get_child_by_value('string_literal')).strip('"').upper())
                else:
                    assert element.get_child_by_value('expression') is not None
                    alias_list.append(str(element.get_child_by_value('expression')).strip('"').upper())
            selected_list_node.add_child(TreeNode(',', src_dialect, True))
        select_list_elements_node = TreeNode('select_list_elements', src_dialect, False)
        expression_node = TreeNode('expression', src_dialect, False)
        expression_node.add_child(TreeNode(op.str_value(), src_dialect, True))

        alias_node = TreeNode('column_alias', src_dialect, False)
        alias_node.add_child(TreeNode(get_used_alias_name(point, alias_list), src_dialect, True))

        select_list_elements_node.add_child(expression_node)

        select_list_elements_node.add_child(alias_node)
        selected_list_node.add_child(select_list_elements_node)
    else:
        assert False


all_schemas = {}

def find_joinable_key(tbl1, tbl2):
    if len(all_schemas) == 0:
        for db_id in get_db_ids():
            with open(os.path.join(get_schema_path(db_id), 'schema.json'), 'r') as file:
                schema = json.load(file)
            for key, value in schema.items():
                all_schemas[key] = value
    col1s = None
    col2s = None
    if tbl1 == tbl2:
        if 'primary_key' in all_schemas[tbl1] and len(all_schemas[tbl1]['primary_key']) > 0:
            col1s = all_schemas[tbl1]['primary_key']
            col2s = all_schemas[tbl2]['primary_key']
        else:
            return None, None
    for fk in all_schemas[tbl1]['foreign_key']:
        ori_column = fk['col']
        ref_table = fk['ref_table']
        ori_ref_column = fk['ref_col']
        if ref_table == tbl2:
            if not isinstance(ori_column, list):
                col1s = [ori_column]
                col2s = [ori_ref_column]
            else:
                col1s = ori_column
                col2s = ori_ref_column
    for fk in all_schemas[tbl2]['foreign_key']:
        ori_column = fk['col']
        ref_table = fk['ref_table']
        ori_ref_column = fk['ref_col']
        if ref_table == tbl1:
            if not isinstance(ori_column, list):
                col1s = [ori_ref_column]
                col2s = [ori_column]
            else:
                col1s = ori_ref_column
                col2s = ori_column
    if col1s is not None:
        for key in col1s:
            if is_any_reserved_keyword(key):
                return None, None
        for key in col2s:
            if is_any_reserved_keyword(key):
                return None, None
    return col1s, col2s


def build_join_cond(used_tbl1, used_tbl2, col1s, col2s):
    assert len(col1s) == len(col2s)
    cond = ''
    for i in range(len(col1s)):
        if cond != '':
            cond += ' AND '
        cond += f"{used_tbl1}.{col1s[i]} = {used_tbl2}.pk{i}"
    return cond


def add_cols_aliases(select_stmt_node, cols: list[str], used_tbl2, dialect) -> list[str]:
    if dialect == 'mysql':
        assert select_stmt_node.value == 'querySpecificationNointo' or select_stmt_node.value == 'querySpecification'
        select_elements_node = select_stmt_node.get_child_by_value('selectElements')
        assert isinstance(select_elements_node, TreeNode)
        if ((len(select_elements_node.children) == 1 and str(select_elements_node) == str(AGGREGATE_FUNCTION_TOKEN)) or
                select_elements_node.get_child_by_value('*') is not None):
            select_elements_node.children = []
        final_name_list = []
        used_names_set = set()
        select_element_nodes = select_elements_node.get_children_by_value('selectElement')
        for select_element_node in select_element_nodes:
            alias = None
            if select_element_node.get_child_by_value('uid') is not None:
                alias = str(select_element_node.get_child_by_value('uid')).strip('`')
            if alias is not None:
                used_names_set.add(alias.upper())
                final_name_list.append(alias)
        for select_element_node in select_element_nodes:
            alias = 'COL'
            idx = 0
            while alias.upper() in used_names_set:
                idx = idx + 1
                alias = 'COL' + str(idx)
            if select_element_node.get_child_by_value('uid') is None:
                select_element_node.add_child(TreeNode(alias, dialect, True))
                used_names_set.add(alias.upper())
                final_name_list.append(alias)
        for i in range(len(cols)):
            if len(select_elements_node.children) > 0:
                select_elements_node.add_child(TreeNode(',', dialect, True))
            select_element_node = TreeNode('selectElement', dialect, False)
            select_element_node.add_child(TreeNode(f'{used_tbl2}.{cols[i]}', dialect, True))
            select_element_node.add_child(TreeNode(f'pk{i}', dialect, True))
            select_elements_node.add_child(select_element_node)
            final_name_list.append(f'pk{i}')
        return final_name_list
    elif dialect == 'pg':
        assert select_stmt_node.value == 'simple_select_pramary'
        target_list_node = select_stmt_node.get_child_by_value('target_list')
        if target_list_node is None:
            opt_target_list = select_stmt_node.get_child_by_value('opt_target_list')
            target_list_node = opt_target_list.get_child_by_value('target_list')
        assert target_list_node is not None
        target_els = target_list_node.get_children_by_value('target_el')
        flag = False
        for target_el in target_els:
            if str(target_el) == '*' or str(target_el) == str(AGGREGATE_FUNCTION_TOKEN):
                flag = True
        used_alias_set = set()
        if flag:
            target_list_node.children = []
        final_name_list = []
        for target_el in target_els:
            if target_el.get_child_by_value('collabel') is not None:
                name = str(target_el.get_child_by_value('collabel')).strip('"')
                used_alias_set.add(name.upper())
                final_name_list.append(name)
            elif target_el.get_child_by_value('identifier') is not None:
                name = str(target_el.get_child_by_value('identifier')).strip('"')
                used_alias_set.add(name.upper())
                final_name_list.append(name)
        for target_el in target_els:
            alias = 'COL'
            idx = 0
            while alias.upper() in used_alias_set:
                idx = idx + 1
                alias = 'COL' + str(idx)
            if target_el.get_child_by_value('collabel') is None or target_el.get_child_by_value('identifier') is None:
                target_el.add_child(TreeNode(alias, dialect, True))
                used_alias_set.add(alias.upper())
                final_name_list.append(alias)
        for i in range(len(cols)):
            if len(target_list_node.children) > 0:
                target_list_node.add_child(TreeNode(',', dialect, True))
            select_element_node = TreeNode('target_el', dialect, False)
            select_element_node.add_child(TreeNode(f'{used_tbl2}.{cols[i]}', dialect, True))
            select_element_node.add_child(TreeNode(f'pk{i}', dialect, True))
            target_list_node.add_child(select_element_node)
            final_name_list.append(f'pk{i}')
        return final_name_list
    elif dialect == 'oracle':
        assert select_stmt_node.value == 'query_block'
        selected_list_node = select_stmt_node.get_child_by_value('selected_list')
        assert selected_list_node is not None
        used_alias_set = set()
        final_name_list = []
        if (selected_list_node.get_child_by_value('*') is not None
                or str(selected_list_node) == str(AGGREGATE_FUNCTION_TOKEN)):
            selected_list_node.children = []
        for element in selected_list_node.get_children_by_value('select_list_elements'):
            if element.get_child_by_value('column_alias') is not None:
                name = str(element.get_child_by_value('column_alias')).strip('"')
                used_alias_set.add(name.upper())
                final_name_list.append(name)
        for element in selected_list_node.get_children_by_value('select_list_elements'):
            alias = 'COL'
            idx = 0
            while alias.upper() in used_alias_set:
                idx = idx + 1
                alias = 'COL' + str(idx)
            if element.get_child_by_value('column_alias') is None:
                element.add_child(TreeNode(alias, dialect, True))
                used_alias_set.add(alias.upper())
                final_name_list.append(alias)
        for i in range(len(cols)):
            if len(selected_list_node.children) > 0:
                selected_list_node.add_child(TreeNode(',', dialect, True))
            select_element_node = TreeNode('select_list_elements', dialect, False)
            select_element_node.add_child(TreeNode(f'{used_tbl2}.{cols[i]}', dialect, True))
            select_element_node.add_child(TreeNode(f'pk{i}', dialect, True))
            selected_list_node.add_child(select_element_node)
            final_name_list.append(f'pk{i}')
        return final_name_list
    else:
        assert False


def fetch_cte_names(root_node, dialect) -> list[str]:
    cte_names = []
    if dialect == 'mysql':
        with_stmt_node = root_node.get_children_by_path(
            ['sqlStatements', 'sqlStatement', 'dmlStatement', 'withStatement'])
        if len(with_stmt_node) != 0:
            assert len(with_stmt_node) == 1
            with_stmt_node = with_stmt_node[0]
            common_table_expressions = with_stmt_node.get_children_by_value('commonTableExpression')
            for cte_root_node in common_table_expressions:
                cte_name = str(cte_root_node.get_child_by_value('cteName')).strip('`')
                cte_names.append(cte_name.upper())
    elif dialect == 'pg':
        select_stmt_node = root_node.get_children_by_path(['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        select_main_node = get_pg_main_select_node_from_select_stmt(select_stmt_node)
        with_clause_node = select_main_node.get_child_by_value('with_clause')
        if with_clause_node is not None:
            cte_nodes = with_clause_node.get_children_by_path(['cte_list', 'common_table_expr'])
            for cte_node in cte_nodes:
                cte_name = str(cte_node.get_child_by_value('name')).strip('"')
                cte_names.append(cte_name.upper())
    elif dialect == 'oracle':
        select_stmt_node = root_node.get_children_by_path(['unit_statement', 'data_manipulation_language_statements',
                                                           'select_statement', 'select_only_statement'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        with_clause_node = select_stmt_node.get_child_by_value('with_clause')
        if with_clause_node is not None:
            cte_nodes = with_clause_node.get_children_by_value('with_factoring_clause')
            for cte_node in cte_nodes:
                query_factoring_clause_node = cte_node.get_child_by_value('subquery_factoring_clause')
                cte_name = str(query_factoring_clause_node.get_child_by_value('query_name')).strip('"')
                cte_names.append(cte_name.upper())
    return cte_names




def join_query2_to_query1(query1_node, query2_node, dialect) -> Optional[TreeNode]:
    # using outer join
    cte_names_query1 = fetch_cte_names(query1_node, dialect)
    cte_names_query2 = fetch_cte_names(query2_node, dialect)
    if dialect == 'mysql':
        query1_select_statement_node = query1_node.get_children_by_path(['sqlStatements', 'sqlStatement',
                                                                         'dmlStatement', 'selectStatement'])
        assert len(query1_select_statement_node) == 1
        query1_select_statement_node = query1_select_statement_node[0]
        query1_select_stmts = fetch_all_simple_select_from_select_stmt_mysql(query1_select_statement_node)
        query2_select_statement_node = query2_node.get_children_by_path(['sqlStatements', 'sqlStatement',
                                                                         'dmlStatement', 'selectStatement'])
        assert len(query2_select_statement_node) == 1
        query2_select_statement_node = query2_select_statement_node[0]
        query2_select_stmts = fetch_all_simple_select_from_select_stmt_mysql(query2_select_statement_node)
        if len(query2_select_stmts) != 1:
            return None
        query2_main_node = query2_select_stmts[0]
        if query2_main_node.get_child_by_value('groupByClause') is not None:
            return None
        for query1_main_node in query1_select_stmts:
            # SQL with groupBy is not suitable for join as lifting the primary key into select list is not suitable
            if query1_main_node.get_child_by_value('groupByClause') is not None:
                return None
            from_clause_node1 = query1_main_node.get_child_by_value('fromClause')
            assert isinstance(from_clause_node1, TreeNode)
            used_subquery_alias1, table_table_used_name_map1, table_table_node_map1 = fetch_used_alias(
                from_clause_node1, dialect, cte_names_query1)
            from_clause_node2 = query2_main_node.get_child_by_value('fromClause')
            assert isinstance(from_clause_node2, TreeNode)
            used_subquery_alias2, table_table_used_name_map2, table_table_node_map2 = fetch_used_alias(
                from_clause_node2, dialect, cte_names_query2)
            flag = False
            used_tbl1 = None
            col1s = None
            col2s = None
            to_join_node = None
            print(table_table_node_map1.keys())
            print(table_table_node_map2.keys())
            for tbl1 in table_table_node_map1:
                used_tbl1 = table_table_used_name_map1[tbl1]
                if get_table_col_name(used_tbl1, dialect) != used_tbl1:
                    continue
                if not flag:
                    for tbl2 in table_table_node_map2:
                        used_tbl2 = table_table_used_name_map2[tbl2]
                        if get_table_col_name(used_tbl2, dialect) != used_tbl2:
                            continue
                        col1s, col2s = find_joinable_key(tbl1, tbl2)
                        if col1s is not None:
                            flag = True
                            to_join_node = table_table_node_map1[tbl1]
                            used_tbl2 = table_table_used_name_map2[tbl2]
                            break
                    if flag:
                        break
            print(col1s, col2s)
            if flag:
                assert isinstance(to_join_node, TreeNode)
                if to_join_node.father.value == 'joinPart':
                    father_node = to_join_node.father
                else:
                    father_node = to_join_node
                used_alias = 'SUBQUERY'
                cnt = 0
                while used_alias in used_subquery_alias1:
                    cnt += 1
                    used_alias = f'SUBQUERY{cnt}'
                added_to_query1_nodes_names = add_cols_aliases(query2_main_node, col2s, used_tbl2, dialect)
                new_tree_node = TreeNode(f' RIGHT OUTER JOIN ({str(query2_main_node)}) '
                                         f'{used_alias} ON {build_join_cond(used_tbl1, used_alias, col1s, col2s)}',
                                         dialect, True)
                father_node.add_child(new_tree_node)
                for node_name in added_to_query1_nodes_names:
                    add_op_to_select(ColumnOp(dialect, node_name, used_alias, BaseType('')),
                                     f'{used_alias}_col', dialect, 'oracle', query1_main_node, True)
                return query1_node
    elif dialect == 'pg':
        query1_select_stmt_node = query1_node.get_children_by_path(['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        assert len(query1_select_stmt_node) == 1
        query1_select_statement_node = query1_select_stmt_node[0]
        query1_select_stmts = fetch_all_simple_select_from_select_stmt_pg(query1_select_statement_node)
        query2_select_statement_node = query2_node.get_children_by_path(
            ['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        assert len(query2_select_statement_node) == 1
        query2_select_statement_node = query2_select_statement_node[0]
        query2_select_stmts = fetch_all_simple_select_from_select_stmt_pg(query2_select_statement_node)
        if len(query2_select_stmts) != 1:
            return None
        query2_main_node = query2_select_stmts[0]
        if query2_main_node.get_child_by_value('group_clause') is not None:
            return None
        for query1_main_node in query1_select_stmts:
            # SQL with groupBy is not suitable for join as lifting the primary key into select list is not suitable
            if query1_main_node.get_child_by_value('group_clause') is not None:
                return None
            from_clause_node1 = query1_main_node.get_child_by_value('from_clause')
            assert isinstance(from_clause_node1, TreeNode)
            used_subquery_alias1, table_table_used_name_map1, table_table_node_map1 = fetch_used_alias(
                from_clause_node1, dialect, cte_names_query1)
            from_clause_node2 = query2_main_node.get_child_by_value('from_clause')
            assert isinstance(from_clause_node2, TreeNode)
            used_subquery_alias2, table_table_used_name_map2, table_table_node_map2 = fetch_used_alias(
                from_clause_node2, dialect, cte_names_query2)
            flag = False
            used_tbl1 = None
            col1s = None
            col2s = None
            to_join_node = None
            for tbl1 in table_table_node_map1:
                used_tbl1 = table_table_used_name_map1[tbl1]
                if get_table_col_name(used_tbl1, dialect) != used_tbl1:
                    continue
                if not flag:
                    for tbl2 in table_table_node_map2:
                        used_tbl2 = table_table_used_name_map2[tbl2]
                        if get_table_col_name(used_tbl2, dialect) != used_tbl2:
                            continue
                        col1s, col2s = find_joinable_key(tbl1, tbl2)
                        if col1s is not None:
                            flag = True
                            to_join_node = table_table_node_map1[tbl1]
                            used_tbl2 = table_table_used_name_map2[tbl2]
                            break
                    if flag:
                        break
            if flag:
                assert isinstance(to_join_node, TreeNode)
                father_node = to_join_node
                while father_node.father.value == 'table_ref':
                    father_node = father_node.father
                used_alias = 'SUBQUERY'
                cnt = 0
                while used_alias in used_subquery_alias1:
                    cnt += 1
                    used_alias = f'SUBQUERY{cnt}'
                added_to_query1_nodes_names = add_cols_aliases(query2_main_node, col2s, used_tbl2, dialect)
                new_tree_node = TreeNode(f' RIGHT OUTER JOIN ({str(query2_main_node)}) '
                                         f'{used_alias} ON {build_join_cond(used_tbl1, used_alias, col1s, col2s)}',
                                         dialect, True)
                father_node.add_child(new_tree_node)
                for node_name in added_to_query1_nodes_names:
                    add_op_to_select(ColumnOp(dialect, node_name, used_alias, BaseType('')),
                                     f'{used_alias}_col', dialect, 'oracle', query1_main_node, True)
                return query1_node
    elif dialect == 'oracle':
        query1_select_stmt_node = query1_node.get_children_by_path(
            ['unit_statement', 'data_manipulation_language_statements',
             'select_statement', 'select_only_statement', 'subquery'])
        assert len(query1_select_stmt_node) == 1
        query1_select_statement_node = query1_select_stmt_node[0]
        query1_select_stmts = fetch_all_simple_select_from_subquery_oracle(query1_select_statement_node)
        query2_select_statement_node = query2_node.get_children_by_path(
            ['unit_statement', 'data_manipulation_language_statements',
             'select_statement', 'select_only_statement', 'subquery'])
        assert len(query2_select_statement_node) == 1
        query2_select_statement_node = query2_select_statement_node[0]
        query2_select_stmts = fetch_all_simple_select_from_subquery_oracle(query2_select_statement_node)
        if len(query2_select_stmts) != 1:
            return None
        query2_main_node = query2_select_stmts[0]
        if query2_main_node.get_child_by_value('group_by_clause') is not None:
            return None
        for query1_main_node in query1_select_stmts:
            # SQL with groupBy is not suitable for join as lifting the primary key into select list is not suitable
            if query1_main_node.get_child_by_value('group_by_clause') is not None:
                return None
            from_clause_node1 = query1_main_node.get_child_by_value('from_clause')
            assert isinstance(from_clause_node1, TreeNode)
            used_subquery_alias1, table_table_used_name_map1, table_table_node_map1 = fetch_used_alias(
                from_clause_node1, dialect, cte_names_query1)
            from_clause_node2 = query2_main_node.get_child_by_value('from_clause')
            assert isinstance(from_clause_node2, TreeNode)
            used_subquery_alias2, table_table_used_name_map2, table_table_node_map2 = fetch_used_alias(
                from_clause_node2, dialect, cte_names_query1)
            flag = False
            used_tbl1 = None
            col1s = None
            col2s = None
            to_join_node = None
            for tbl1 in table_table_node_map1:
                used_tbl1 = table_table_used_name_map1[tbl1]
                if get_table_col_name(used_tbl1, dialect) != used_tbl1:
                    continue
                if not flag:
                    for tbl2 in table_table_node_map2:
                        used_tbl2 = table_table_used_name_map2[tbl2]
                        if get_table_col_name(used_tbl2, dialect) != used_tbl2:
                            continue
                        col1s, col2s = find_joinable_key(tbl1, tbl2)
                        if col1s is not None:
                            flag = True
                            to_join_node = table_table_node_map1[tbl1]
                            used_tbl2 = table_table_used_name_map2[tbl2]
                            break
                    if flag:
                        break
            if flag:
                assert isinstance(to_join_node, TreeNode)
                father_node = to_join_node
                while father_node.father.value == 'table_ref':
                    father_node = father_node.father
                used_alias = 'SUBQUERY'
                cnt = 0
                while used_alias in used_subquery_alias1:
                    cnt += 1
                    used_alias = f'SUBQUERY{cnt}'
                added_to_query1_nodes_names = add_cols_aliases(query2_main_node, col2s, used_tbl2, dialect)
                new_tree_node = TreeNode(f' RIGHT OUTER JOIN ({str(query2_main_node)}) '
                                         f'{used_alias} ON {build_join_cond(used_tbl1, used_alias, col1s, col2s)}',
                                         dialect, True)
                father_node.add_child(new_tree_node)
                for node_name in added_to_query1_nodes_names:
                    add_op_to_select(ColumnOp(dialect, node_name, used_alias, BaseType('')),
                                     f'{used_alias}_col', dialect, 'oracle', query1_main_node, True)
                return query1_node
    else:
        assert False
    return None


def join_queries(query1, query2, dialect) -> Optional[TreeNode]:
    query1_node, _, _, _ = parse_tree(query1, dialect)
    query2_node, _, _, _ = parse_tree(query2, dialect)
    query1_node = TreeNode.make_g4_tree_by_node(query1_node, dialect)
    query2_node = TreeNode.make_g4_tree_by_node(query2_node, dialect)
    if query1_node is None or query2_node is None:
        return None
    node = join_query2_to_query1(query1_node, query2_node, dialect)
    if node is None:
        query1_node, _, _, _ = parse_tree(query1, dialect)
        query2_node, _, _, _ = parse_tree(query2, dialect)
        query1_node = TreeNode.make_g4_tree_by_node(query1_node, dialect)
        query2_node = TreeNode.make_g4_tree_by_node(query2_node, dialect)
        return join_query2_to_query1(query2_node, query1_node, dialect)
    return node


def merge_query(query_dict1: dict | None, query_dict2: dict,
                execute_env: ExecutionEnv, del_mode: bool = False, use_null_flag: bool = False):
    cte_alias_set = set()
    if query_dict1 is None:
        return query_dict2
    src_dialect = None
    tgt_dialect = None
    for key, value in query_dict1.items():
        if src_dialect is None:
            src_dialect = key
        elif tgt_dialect is None:
            tgt_dialect = key
    query1 = query_dict1[src_dialect]
    query2 = query_dict2[src_dialect]
    points = merge_trans_points(query_dict1['points'], query_dict2['points'])
    joined_query = join_queries(query1, query2, execute_env.dialect)
    if joined_query is not None:
        flag, res = execute_env.explain_execute_sql(str(joined_query))
        if flag:
            print('Join Success')
            return {
                src_dialect: str(joined_query),
                tgt_dialect: '',
                "points": points
            }
        else:
            print('join fail', res)
            print(joined_query)

    query1, query2 = process_to_add_sql(None, query1, query2, src_dialect,
                                        execute_env, del_mode, use_null_flag)
    if query1 is None:
        return None
    query1_root_node, _, _, _ = parse_tree(query1, src_dialect)
    if query1_root_node is None:
        return None
    else:
        query1_root_node = TreeNode.make_g4_tree_by_node(query1_root_node, src_dialect)
    query2_root_node, _, _, _ = parse_tree(query2, src_dialect)
    if query2_root_node is None:
        return None
    else:
        query2_root_node = TreeNode.make_g4_tree_by_node(query2_root_node, src_dialect)

    flag1, query1_ctes = analysis_ctes(query1_root_node, src_dialect)
    flag2, query2_ctes = analysis_ctes(query2_root_node, src_dialect)
    flag = True
    for cte1 in query1_ctes['cte_list']:
        cte_alias_set.add(cte1['cte_name'])
        for cte2 in query2_ctes['cte_list']:
            cte_alias_set.add(cte2['cte_name'])
            if cte1['cte_name'] == cte2['cte_name']:
                flag = False
    if not flag:
        return None
    final_ctes = {
        "is_recursive": query1_ctes['is_recursive'] and query2_ctes['is_recursive'],
        'cte_list': query1_ctes['cte_list'] + query2_ctes['cte_list']
    }

    query1_select_body_node = fetch_query_body_node(query1_root_node, src_dialect)
    query2_select_body_node = fetch_query_body_node(query2_root_node, src_dialect)
    query_body_str = ''
    if has_limit_order_by(query1_select_body_node, src_dialect):
        i = 0
        while f'cte{i}' in cte_alias_set:
            i += 1
        cte_alias_set.add(f'cte{i}')
        final_ctes['cte_list'].append({
            'cte_name': f"cte{i}",
            'query': str(query1_select_body_node),
            'column_list': None,
            'cte_name_type_pairs': []
        })
        query_body_str += f'SELECT * FROM cte{i}'
    else:
        query_body_str = str(query1_select_body_node)
    if has_limit_order_by(query2_select_body_node, src_dialect):
        i = 0
        while f'cte{i}' in cte_alias_set:
            i += 1
        final_ctes['cte_list'].append({
            'cte_name': f"cte{i}",
            'query': str(query2_select_body_node),
            'column_list': None,
            'cte_name_type_pairs': []
        })
        query_body_str += f' UNION ALL SELECT * FROM cte{i}'
    else:
        query_body_str += f' UNION ALL {str(query2_select_body_node)}'
    merged_query = build_ctes(final_ctes, src_dialect) + ' ' + query_body_str
    return {
        src_dialect: merged_query.strip(),
        tgt_dialect: '',
        "points": points
    }


def points_equal(points1: list[dict], points2: list[dict]):
    if len(points1) != len(points2):
        return False
    for p1 in points1:
        flag = False
        for p2 in points2:
            if p1['point'] == p2['point'] and p1['num'] == p2['num']:
                flag = True
        if not flag:
            return False
    return True
