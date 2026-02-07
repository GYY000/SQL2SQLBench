import json
import os
import random

from tqdm import tqdm

from antlr_parser.Tree import TreeNode
from antlr_parser.mysql_tree import fetch_all_simple_select_from_select_stmt_mysql
from antlr_parser.oracle_tree import fetch_all_simple_select_from_subquery_oracle
from antlr_parser.parse_tree import parse_tree
from antlr_parser.pg_tree import dfs_select_clause
from sql_gen.generator.add_pattern_point import analyze_select_stmt, analyze_sql_statement
from sql_gen.generator.add_point import try_gen_function_point_sql
from sql_gen.generator.ele_type.operand_analysis import analysis_sql
from sql_gen.generator.pattern_tree_parser import parse_pattern_tree
from sql_gen.generator.point_loader import load_translation_point, load_point_by_name
from sql_gen.generator.point_parser import parse_point
from sql_gen.generator.point_type.TranPointType import ClauseType, ExpressionType, LiteralType, ReservedKeywordType
from utils.CISpacelessSet import CISpacelessSet
from utils.ExecutionEnv import ExecutionEnv
from utils.db_connector import snowflake_sql_execute, sqlserver_sql_execute
from utils.tools import get_all_db_name, get_db_ids, get_data_path, get_proj_root_path


def mark_sql_fulfilled_point(sqls: list[dict], point, src_dialect, op_analysis_results, ori_p):
    print(f'Marking point {point.point_name}')
    if isinstance(point.point_type, ClauseType):
        try:
            src_pattern_tree = parse_pattern_tree(point.point_type, point.src_pattern, point.src_dialect)
        except Exception as e:
            return
        if src_dialect == 'mysql':
            if src_pattern_tree.value == 'querySpecification' or src_pattern_tree.value == 'querySpecificationNointo':
                select_pattern = analyze_select_stmt(src_pattern_tree, src_dialect)
            else:
                node = src_pattern_tree
                if node.value == 'root':
                    node = node.get_child_by_value('sqlStatements')
                    assert node is not None
                if node.value == 'sqlStatements':
                    node = node.get_child_by_value('sqlStatement')
                    assert node is not None
                if node.value == 'sqlStatement':
                    node = node.get_child_by_value('dmlStatement')
                    assert node is not None
                if node.value == 'dmlStatement':
                    node = node.get_child_by_value('selectStatement')
                    assert node is not None
                simple_select_nodes = fetch_all_simple_select_from_select_stmt_mysql(node)
                if len(simple_select_nodes) != 1:
                    return
                select_pattern = analyze_select_stmt(simple_select_nodes[0], src_dialect)
        elif src_dialect == 'oracle':
            if src_pattern_tree.value == 'query_block':
                select_pattern = analyze_select_stmt(src_pattern_tree, src_dialect)
            else:
                node = src_pattern_tree
                if node.value == 'sql_script':
                    node = node.get_child_by_value('unit_statement')
                if node.value == 'unit_statement':
                    node = node.get_child_by_value('data_manipulation_language_statements')
                if node.value == 'select_statement':
                    node = node.get_child_by_value('select_only_statement')
                if node.value == 'select_only_statement':
                    node = node.get_child_by_value('subquery')
                assert node is not None
                simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(node)
                if len(simple_select_nodes) != 1:
                    return
                select_pattern = analyze_select_stmt(simple_select_nodes[0], src_dialect)
        elif src_dialect == 'pg':
            if src_pattern_tree.value == 'simple_select_pramary':
                select_pattern = analyze_select_stmt(src_pattern_tree, src_dialect)
            else:
                node = src_pattern_tree
                if node.value == 'root':
                    node = node.get_child_by_value('stmtblock')
                    assert node is not None
                if node.value == 'stmtblock':
                    node = node.get_child_by_value('stmtmulti')
                    assert node is not None
                if node.value == 'stmtmulti':
                    node = node.get_child_by_value('stmt')
                    assert node is not None
                if node.value == 'stmt':
                    node = node.get_child_by_value('selectstmt')
                    assert node is not None
                if node.value != 'select_no_parens':
                    while node.get_child_by_value('select_no_parens') is None:
                        assert node.get_child_by_value('select_with_parens') is not None
                        node = node.get_child_by_value('select_with_parens')
                    assert node.get_child_by_value('select_no_parens') is not None
                    node = node.get_child_by_value('select_no_parens')
                select_clause_node = node.get_child_by_value('select_clause')
                select_stmts_tbd = dfs_select_clause(select_clause_node)
                if len(select_stmts_tbd) != 1:
                    return
                select_pattern = analyze_select_stmt(select_stmts_tbd[0], src_dialect)
        else:
            assert False
        for sql in tqdm(sqls):
            try:
                tree_node, _, _, _ = parse_tree(sql[src_dialect], src_dialect)
                assert tree_node is not None
                tree_node = TreeNode.make_g4_tree_by_node(tree_node, src_dialect)
                sql_components = analyze_sql_statement(tree_node, src_dialect)
                if sql_components is None:
                    continue
                if select_pattern.where_cond_variable is not None and sql_components['where_cond'] is None:
                    continue
                if (select_pattern.group_by_cols_variable is not None and
                        len(select_pattern.group_by_cols_variable) != 0 and
                        len(sql_components['group_by_cols']) == 0):
                    continue
                if select_pattern.having_cond_variable is not None and sql_components['having_cond'] is None:
                    continue
                if len(select_pattern.from_tables_variable) != 0 and len(sql_components['from_tables']) == 0:
                    continue
                if len(sql_components['group_by_cols']) != 0 and select_pattern.group_by_cols_variable is None:
                    continue
                final_flag = True
                for var in select_pattern.from_tables_variable:
                    if isinstance(var, dict):
                        flag = False
                        for table_ref in sql_components['from_tables']:
                            if table_ref.get_child_by_value('join_qual') is not None:
                                flag = True
                                break
                        if not flag:
                            final_flag = False
                            break
                if not final_flag:
                    continue
                sql['fulfilling_points'].append(point.point_name)
            except Exception as e:
                continue
    elif isinstance(point.point_type, ExpressionType) or isinstance(point.point_type, LiteralType):
        used_point = parse_point(ori_p)
        if len(op_analysis_results) == 0:
            print('---Analyze all sqls!---')
            cnt = 0
            for sql in tqdm(sqls):
                try:
                    op_analysis_res = analysis_sql(sql[src_dialect], src_dialect)
                except Exception as e:
                    op_analysis_res = None
                op_analysis_results[sql[src_dialect]] = op_analysis_res
                cnt += 1
        execution_env = ExecutionEnv(src_dialect, get_all_db_name(src_dialect))
        if used_point.tag is not None and 'DB PARAMETER' in used_point.tag:
            for key, value in used_point.tag['DB PARAMETER'].items():
                flag = execution_env.add_param(key, value)
                if not flag:
                    assert False
        for sql in tqdm(sqls):
            try:
                for i in range(3):
                    # try three times to mark whether a sql can be generated as there are randomness
                    try:
                        # restore_op_analysis_result
                        sql_analysis_res = op_analysis_results[sql[src_dialect]]
                        if sql_analysis_res is None:
                            continue
                        restore_cols_dict = {}
                        restore_group_cols_dict = {}
                        for select_stmt in sql_analysis_res['select_stmts']:
                            new_cols = []
                            for col in select_stmt['cols']:
                                new_cols.append(col)
                            restore_cols_dict[str(select_stmt['select_root_node'])] = new_cols
                            if select_stmt.get('group_by_cols', None) is not None:
                                new_groups = []
                                for col in select_stmt['group_by_cols']:
                                    new_groups.append(col)
                                restore_group_cols_dict[str(select_stmt['select_root_node'])] = new_groups
                        res = try_gen_function_point_sql(sql, used_point, [], False, execution_env, CISpacelessSet(),
                                                         op_analysis_results[sql[src_dialect]], False)
                        for select_stmt in sql_analysis_res['select_stmts']:
                            select_stmt['cols'] = restore_cols_dict[str(select_stmt['select_root_node'])]
                            if select_stmt.get('group_by_cols', None) is not None:
                                select_stmt['group_by_cols'] = restore_group_cols_dict[
                                    str(select_stmt['select_root_node'])]
                    except Exception as e:
                        continue
                    if res is not None:
                        sql['fulfilling_points'].append(used_point.point_name)
                        break
            except Exception as e:
                continue
    elif isinstance(point.point_type, ReservedKeywordType):
        word, _ = point.src_pattern.extend_pattern()
        reserved_keyword = word
        db_ids = get_db_ids()
        reserved_keyword_list = []
        for db_id in db_ids:
            with open(os.path.join(get_data_path(), db_id, 'schema.json')) as file:
                schema = json.load(file)
                for table, table_content in schema.items():
                    assert isinstance(table, str)
                    if table.upper() == reserved_keyword.upper():
                        fk_tables = []
                        for tbl1, tbl_cont1 in schema.items():
                            if tbl1 == table:
                                continue
                            for fk in tbl_cont1['foreign_key']:
                                if fk['ref_table'] == table and isinstance(fk['col'], str):
                                    fk_tables.append({
                                        "RefTable": tbl1,
                                        "RefCol": fk['col'],
                                        "Col": fk['ref_col']
                                    })
                        for fk in table_content['foreign_key']:
                            if isinstance(fk['col'], str):
                                fk_tables.append({
                                    "RefTable": fk['ref_table'],
                                    "RefCol": fk['ref_col'],
                                    "Col": fk['col']
                                })
                        random.shuffle(fk_tables)
                        reserved_keyword_list.append({
                            "Type": "Table",
                            "TableName": table,
                            "ForeignKeyTables": fk_tables
                        })
                    for col in table_content['cols']:
                        if col['col_name'].upper() == reserved_keyword.upper():
                            reserved_keyword_list.append({
                                "Type": "Column",
                                "TableName": table,
                                "ColumnName": col['col_name']
                            })
        random.shuffle(reserved_keyword_list)
        for ele in reserved_keyword_list:
            if ele['Type'] == 'Table':
                for sql in tqdm(sqls):
                    sql['fulfilling_points'].append(point.point_name)
            else:
                for sql in tqdm(sqls):
                    if 'tables' not in sql:
                        continue
                    if ele['TableName'] in sql['tables']:
                        sql['fulfilling_points'].append(point.point_name)


def mark_dialect(src_dialect, tgt_dialect):
    marked_points = []
    if os.path.exists(f'/home/gyy/SQL2SQL_Bench/conv_point/{src_dialect}_{tgt_dialect}_points_marked.json'):
        with open(f'/home/gyy/SQL2SQL_Bench/conv_point/{src_dialect}_{tgt_dialect}_points_marked.json', 'r') as f:
            marked_points = json.load(f)
    points = load_translation_point(src_dialect, tgt_dialect)
    to_mark_points = []
    for key, value in points.items():
        for p in value:
            if p['Desc'] in marked_points:
                continue
            else:
                to_mark_points.append(p)
    not_marked_points = stat_mark_state(src_dialect, tgt_dialect)
    for p in not_marked_points:
        to_mark_points.append(load_point_by_name(src_dialect, tgt_dialect, p))
    enhanced_sql_file_path = f'/home/gyy/SQL2SQL_Bench/SQL/{src_dialect}_{tgt_dialect}_marked.json'
    if os.path.exists(enhanced_sql_file_path):
        with open(enhanced_sql_file_path, 'r') as f:
            sqls = json.load(f)
    else:
        with open(f'/home/gyy/SQL2SQL_Bench/SQL/{src_dialect}_{tgt_dialect}.json', 'r') as f:
            sqls = json.load(f)
    for sql in sqls:
        if 'fulfilling_points' not in sql:
            sql['fulfilling_points'] = []
    op_analysis_results = {}
    for point in tqdm(to_mark_points):
        try:
            parsed_p = parse_point(point)
        except Exception as e:
            continue
        mark_sql_fulfilled_point(sqls, parsed_p, src_dialect, op_analysis_results, point)
        with open(enhanced_sql_file_path, 'w') as f:
            json.dump(sqls, f, indent=4, ensure_ascii=False)
        if parsed_p.point_name not in marked_points:
            marked_points.append(parsed_p.point_name)
        with open(f'/home/gyy/SQL2SQL_Bench/conv_point/{src_dialect}_{tgt_dialect}_points_marked.json', 'w') as f:
            json.dump(marked_points, f, indent=4)


def stat_mark_state(src_dialect, tgt_dialect):
    enhanced_sql_file_path = f'/home/gyy/SQL2SQL_Bench/SQL/{src_dialect}_{tgt_dialect}_marked.json'
    if os.path.exists(enhanced_sql_file_path):
        with open(enhanced_sql_file_path, 'r') as f:
            sqls = json.load(f)
    else:
        enhanced_sql_file_path = f'/home/gyy/SQL2SQL_Bench/SQL/{src_dialect}_{tgt_dialect}.json'
        with open(enhanced_sql_file_path, 'r') as f:
            sqls = json.load(f)
    point_sets = set()
    for sql in sqls:
        for p in sql.get('fulfilling_points', []):
            point_sets.add(p)
    print(src_dialect, tgt_dialect)
    print(len(point_sets))
    if os.path.exists(f'/home/gyy/SQL2SQL_Bench/conv_point/{src_dialect}_{tgt_dialect}_points_marked.json'):
        with open(f'/home/gyy/SQL2SQL_Bench/conv_point/{src_dialect}_{tgt_dialect}_points_marked.json', 'r') as f:
            marked_points = json.load(f)
    else:
        marked_points = []
    not_marked_points = []
    for point in marked_points:
        if point not in point_sets:
            print(point)
            not_marked_points.append(point)
    return not_marked_points

def mark_new_sqls_only(src_dialect, tgt_dialect):
    marked_points = []
    if os.path.exists(f'/home/gyy/SQL2SQL_Bench/conv_point/{src_dialect}_{tgt_dialect}_points_marked.json'):
        with open(f'/home/gyy/SQL2SQL_Bench/conv_point/{src_dialect}_{tgt_dialect}_points_marked.json', 'r') as f:
            marked_points = json.load(f)
    points = load_translation_point(src_dialect, tgt_dialect)
    to_mark_points = []
    for key, value in points.items():
        for p in value:
            to_mark_points.append(p)
    enhanced_sql_file_path = f'/home/gyy/SQL2SQL_Bench/SQL/{src_dialect}_{tgt_dialect}_marked.json'
    if os.path.exists(enhanced_sql_file_path):
        with open(enhanced_sql_file_path, 'r') as f:
            sqls = json.load(f)
    else:
        with open(f'/home/gyy/SQL2SQL_Bench/SQL/{src_dialect}_{tgt_dialect}.json', 'r') as f:
            sqls = json.load(f)
    to_mark_sqls = []
    for sql in sqls:
        if 'fulfilling_points' not in sql:
            sql['fulfilling_points'] = []
            to_mark_sqls.append(sql)
    op_analysis_results = {}
    for point in tqdm(to_mark_points):
        try:
            parsed_p = parse_point(point)
        except Exception as e:
            continue
        mark_sql_fulfilled_point(to_mark_sqls, parsed_p, src_dialect, op_analysis_results, point)
        with open(enhanced_sql_file_path, 'w') as f:
            json.dump(sqls, f, indent=4, ensure_ascii=False)
        if parsed_p.point_name not in marked_points:
            marked_points.append(parsed_p.point_name)
        with open(f'/home/gyy/SQL2SQL_Bench/conv_point/{src_dialect}_{tgt_dialect}_points_marked.json', 'w') as f:
            json.dump(marked_points, f, indent=4)



# dialects = ['mysql', 'pg', 'oracle']
# for src_dialect in dialects:
#     for tgt_dialect in dialects:
#         if src_dialect != tgt_dialect:
#             stat_mark_state(src_dialect, tgt_dialect)
#
# stat_mark_state('oracle', 'snowflake')

def merge_all_sqls_into_one_file(src_dialect, tgt_dialect):
    all_sqls = []
    for db in ['tpch']:
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
                sqls = []
            assert sqls is not None
            all_sqls = all_sqls + sqls
    #     path = os.path.join(sql_root_path, 'points', f'{src_dialect}_{tgt_dialect}.json')
    #     if os.path.exists(path):
    #         with open(path, 'r') as file:
    #             sqls = json.load(file)
    #         for sql in sqls:
    #             flag = True
    #             for point in sql['points']:
    #                 if point not in points:
    #                     flag = False
    #                     break
    #             if flag:
    #                 all_sqls.append(sql)
    # for sql in all_sqls:
    #     if 'points' not in sql:
    #         sql['points'] = []
    if os.path.exists(f'/home/gyy/SQL2SQL_Bench/SQL/{src_dialect}_{tgt_dialect}_marked.json'):
        with open(f'/home/gyy/SQL2SQL_Bench/SQL/{src_dialect}_{tgt_dialect}_marked.json', 'r') as f:
            marked_sqls = json.load(f)
        for sql in all_sqls:
            flag = False
            for marked_sql in marked_sqls:
                if sql[src_dialect] == marked_sql[src_dialect]:
                    flag = True
                    break
            if not flag:
                marked_sqls.append(sql)
    else:
        marked_sqls = all_sqls
    with open(f'/home/gyy/SQL2SQL_Bench/SQL/{src_dialect}_{tgt_dialect}_marked.json', 'w') as f:
        json.dump(marked_sqls, f, indent=4, ensure_ascii=False)

def point_check(src_dialect, tgt_dialect):
    points = load_translation_point(src_dialect, tgt_dialect)
    to_mark_points = []
    for key, value in points.items():
        for p in value:
            print(p['Desc'])
            parse_point(p)

# dialects = ['mysql', 'pg', 'oracle']
# for src_dialect in dialects:
#     for tgt_dialect in dialects:
#         if src_dialect == tgt_dialect:
#             continue
#         merge_all_sqls_into_one_file(src_dialect, tgt_dialect)
# dialects = ['mysql', 'pg', 'oracle']
# for src_dialect in dialects:
#     for tgt_dialect in dialects:
#         if src_dialect == tgt_dialect:
#             continue
#         mark_new_sqls_only(src_dialect, tgt_dialect)
mark_new_sqls_only('mysql', 'pg')
mark_new_sqls_only('pg', 'mysql')
