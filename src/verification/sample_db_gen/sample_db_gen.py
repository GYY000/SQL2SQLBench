import threading
from typing import Optional

from antlr_parser.Tree import TreeNode
from antlr_parser.general_tree_analysis import dfs_table_ref_node
from antlr_parser.mysql_tree import fetch_all_simple_select_from_select_stmt_mysql
from antlr_parser.oracle_tree import fetch_all_simple_select_from_subquery_oracle
from antlr_parser.parse_tree import parse_tree
from antlr_parser.pg_tree import get_pg_main_select_node_from_select_stmt, fetch_all_simple_select_from_select_stmt_pg
from db_builder.schema_builder import drop_schema, schema_build, create_table
from model.model_init import init_model
from sql_gen.generator.ele_type.type_conversion import type_mapping
from sql_gen.generator.ele_type.type_operation import build_value

from utils.db_connector import sql_execute, get_mysql_type, get_oracle_type, get_pg_type, sql_dependent_execute
from utils.tools import get_table_col_name, get_db_ids, get_all_db_name, load_db_config
from verification.comparision import tol_order_unaware_compare, tol_order_aware_compare
from verification.sample_db_gen.prompt import GEN_PROMPT

MAGIC_SUBQUERY_TOKEN = 'SUBQUERYQUERY'


class SubTreeNode:
    def __init__(self, tree_node: TreeNode | None, table_name: str | None = None, dialect: str = 'mysql', db_param: dict = None):
        self.tree_node = tree_node
        self.table_name = table_name
        self.children = []
        self.schema = None
        self.dialect = dialect
        self.subquery_id_map = {}
        self.subquery_idx = 0
        self.db_param = db_param
        self.ddls = {}

    def add_child_node(self, child_node):
        for child in self.children:
            if child.is_table() and child_node.is_table() and child.table_name == child_node.table_name:
                return
            elif not child.is_table() and not child_node.is_table() and child.tree_node == child_node.tree_node:
                return
        self.children.append(child_node)
        if not child_node.is_table():
            self.subquery_id_map[child_node] = f'{MAGIC_SUBQUERY_TOKEN}{self.subquery_idx}'
            self.subquery_idx += 1
        else:
            self.subquery_id_map[child_node.table_name] = child_node.table_name

    def get_existing_data(self, db_data):
        existing_data = {}
        for child in self.children:
            if child.is_table() and child.table_name in db_data:
                existing_data[child.table_name] = db_data[child.table_name]
        return existing_data


    def is_table(self):
        return self.table_name is not None

    def combine_child_node(self):
        assert len(self.children) == 1
        # No need to modify self.treenode just keep the uppermost layer
        while len(self.children) == 1:
            self.children = self.children[0].children
            self.subquery_id_map = self.children[0].subquery_id_map
            self.subquery_idx = self.children[0].subquery_idx
            if self.schema is None:
                self.schema = self.children[0].schema
                self.ddls = self.children[0].ddls
            self.table_name = self.children[0].table_name

    def get_schema(self, used_id=None):
        if self.schema is not None:
            return self.schema, self.ddls
        if self.table_name is not None:
            db_ids = get_db_ids()
            for db_id in db_ids:
                schema, add_constraints, type_defs = schema_build(db_id, self.dialect)
                if self.table_name in schema:
                    self.schema = {self.table_name: schema[self.table_name]}
                    create_stmt = create_table(schema[self.table_name], copy.deepcopy(add_constraints[self.table_name]), self.dialect)
                    self.ddls = {self.table_name: [create_stmt] + type_defs[self.table_name]}
        else:
            self.schema = {}
            for child in self.children:
                schema, ddls = child.get_schema(self.subquery_id_map.get(child, None))
                for key, value in schema.items():
                    self.schema[key] = value
                for key, value in ddls.items():
                    self.ddls[key] = value
            cols = []
            if self.dialect == 'mysql':
                flag, types = get_mysql_type(get_all_db_name(self.dialect), self.get_revised_sql(), False, self.db_param)
            elif self.dialect == 'oracle':
                flag, types = get_oracle_type(get_all_db_name(self.dialect), self.get_revised_sql(), False, self.db_param)
            elif self.dialect == 'pg':
                flag, types = get_pg_type(get_all_db_name(self.dialect), self.get_revised_sql(), False, self.db_param)
            else:
                assert False
            if not flag:
                print(types)
            j = 0
            while j < len(types):
                cols.append({
                    "col_name": types[j]['col'],
                    "type": type_mapping(self.dialect, types[j]['type']),
                    "attribute": [],
                    "semantic": {}
                })
                j += 1
            if used_id is not None:
                self.schema[used_id] = {
                    "table": used_id,
                    "cols": cols,
                    "primary_key": [],
                    "foreign_key": [],
                    "index": []
                }
                create_stmt = create_table(self.schema[used_id], [], self.dialect)
                self.ddls[self.table_name] = [create_stmt]
        return self.schema, self.ddls

    def get_revised_sql(self):
        # replacing subquery with e.g, SUBQUERY1
        assert not self.is_table()
        ctes = []
        ori_child_values_map = {}
        for child in self.children:
            if not child.is_table():
                cte_body = str(child.tree_node).strip()
                if cte_body.startswith('('):
                    cte_body = cte_body[1:-1]
                ctes.append(
                    {
                        "cte_name": self.subquery_id_map[child],
                        "cte_body": cte_body
                    }
                )
                ori_child_values_map[child] = child.tree_node.value
                child.tree_node.is_terminal = True
                child.tree_node.value = self.subquery_id_map[child]
        if len(ctes) > 0:
            cte_stmt = 'WITH '
            for cte in ctes:
                cte_stmt += f"{cte['cte_name']} AS ({cte['cte_body']})\n"
        else:
            cte_stmt = ''
        res = cte_stmt + str(self.tree_node)
        for key, value in ori_child_values_map.items():
            key.tree_node.value = value
            key.tree_node.is_terminal = False
        return res


def find_in_stack(cte_stack, name):
    i = len(cte_stack) - 1
    while i >= 0:
        if name in cte_stack[i]:
            return cte_stack[i][name]
        i -= 1
    return None


def merge_subtree_nodes(root_node: SubTreeNode):
    if len(root_node.children) == 1:
        root_node.combine_child_node()
    for child in root_node.children:
        merge_subtree_nodes(child)


def analyze_subtree_nodes(select_stmt_node: TreeNode, cte_names: list[dict], dialect,
                          cur_subtree_node: Optional[SubTreeNode], db_param):
    cur_layer_tables = {}
    if dialect == 'mysql':
        from_clause_node = select_stmt_node.get_child_by_value('fromClause')
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
                    if table_source_item_node.get_child_by_value('LATERAL') is not None:
                        cte_names.append(cur_layer_tables)
                    query_body_node = table_source_item_node.get_child_by_value('dmlStatement')
                    select_statement_node = query_body_node.get_child_by_value('selectStatement')
                    sub_tree_node = SubTreeNode(select_statement_node, dialect=dialect, db_param=db_param)
                    select_stmts = fetch_all_simple_select_from_select_stmt_mysql(select_statement_node)
                    for simple_select_stmt_node in select_stmts:
                        analyze_subtree_nodes(simple_select_stmt_node, cte_names, dialect, sub_tree_node, db_param)
                    merge_subtree_nodes(sub_tree_node)
                    cur_subtree_node.add_child_node(sub_tree_node)
                    if table_source_item_node.get_child_by_value('LATERAL') is not None:
                        cte_names.remove(cur_layer_tables)
                    if table_source_item_node.get_child_by_value('uid') is not None:
                        name = str(table_source_item_node.get_child_by_value('uid')).strip('`')
                        cur_layer_tables[name] = sub_tree_node
                else:
                    assert table_source_item_node.get_child_by_value('tableName') is not None
                    name = str(table_source_item_node.get_child_by_value('tableName'))
                    name = name.strip('`')
                    if table_source_item_node.get_child_by_value('uid') is not None:
                        used_name = str(table_source_item_node.get_child_by_value('uid')).strip('`')
                    else:
                        used_name = name
                    existing_sub_query = find_in_stack(cte_names, name)
                    if existing_sub_query is not None and not isinstance(existing_sub_query, str):
                        cur_subtree_node.add_child_node(existing_sub_query)
                        cur_layer_tables[used_name] = existing_sub_query
                    else:
                        if existing_sub_query is not None:
                            table_name = existing_sub_query
                        else:
                            table_name = name
                        sub_tree_node = SubTreeNode(None, table_name, dialect=dialect, db_param=db_param)
                        cur_subtree_node.add_child_node(sub_tree_node)
                        cur_layer_tables[used_name] = table_name
    elif dialect in ['pg', 'sqlserver', 'snowflake']:
        from_clause_node = select_stmt_node.get_child_by_value('from_clause')
        if from_clause_node is not None:
            assert isinstance(from_clause_node, TreeNode)
            table_ref_nodes = from_clause_node.get_children_by_path(['from_list', 'table_ref'])
            all_table_ref_nodes = []
            for table_ref_node in table_ref_nodes:
                all_table_ref_nodes += dfs_table_ref_node(table_ref_node)
            for table_ref_node in all_table_ref_nodes:
                if table_ref_node.get_child_by_value('relation_expr') is not None:
                    name = str(table_ref_node.get_child_by_value('relation_expr')).strip('"').strip('[').strip(']')
                    if table_ref_node.get_child_by_value('opt_alias_clause') is not None:
                        alias_name = table_ref_node.get_children_by_path(
                            ['opt_alias_clause', 'table_alias_clause', 'table_alias'])
                        assert len(alias_name) > 0
                        used_name = str(alias_name[0]).strip('"').strip('[').strip(']')
                    else:
                        used_name = name
                    existing_sub_query = find_in_stack(cte_names, name)
                    if existing_sub_query is not None and not isinstance(existing_sub_query, str):
                        cur_subtree_node.add_child_node(existing_sub_query)
                        cur_layer_tables[used_name] = existing_sub_query
                    else:
                        if existing_sub_query is not None:
                            table_name = existing_sub_query
                        else:
                            table_name = name
                        sub_tree_node = SubTreeNode(None, table_name, dialect=dialect, db_param=db_param)
                        cur_subtree_node.add_child_node(sub_tree_node)
                        cur_layer_tables[used_name] = table_name
                elif table_ref_node.get_child_by_value('select_with_parens') is not None:
                    if table_ref_node.get_child_by_value('LATERAL') is not None:
                        cte_names.append(cur_layer_tables)
                    select_stmt_node = table_ref_node.get_child_by_value('select_with_parens')
                    sub_tree_node = SubTreeNode(select_stmt_node, dialect=dialect, db_param=db_param)
                    select_stmt_nodes = fetch_all_simple_select_from_select_stmt_pg(
                        table_ref_node.get_child_by_value('select_with_parens'))
                    for simple_select_stmt_node in select_stmt_nodes:
                        analyze_subtree_nodes(simple_select_stmt_node, cte_names, dialect, sub_tree_node, db_param)

                    if table_ref_node.get_child_by_value('opt_alias_clause') is not None:
                        alias_name = table_ref_node.get_children_by_path(
                            ['opt_alias_clause', 'table_alias_clause', 'table_alias'])
                        assert len(alias_name) > 0
                        name = str(alias_name[0]).strip('"').strip('[').strip(']')
                        cur_layer_tables[name] = sub_tree_node
                    if table_ref_node.get_child_by_value('LATERAL') is not None:
                        cte_names.remove(cur_layer_tables)
                    cur_subtree_node.add_child_node(sub_tree_node)
    elif dialect == 'oracle':
        from_clause_node = select_stmt_node.get_child_by_value('from_clause')
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
                        used_name = str(alias_node).strip('"')
                    else:
                        used_name = name
                    existing_sub_query = find_in_stack(cte_names, name)
                    if existing_sub_query is not None and not isinstance(existing_sub_query, str):
                        cur_subtree_node.add_child_node(existing_sub_query)
                        cur_layer_tables[used_name] = existing_sub_query
                    else:
                        if existing_sub_query is not None:
                            table_name = existing_sub_query
                        else:
                            table_name = name
                        sub_tree_node = SubTreeNode(None, table_name, dialect=dialect, db_param=db_param)
                        cur_subtree_node.add_child_node(sub_tree_node)
                        cur_layer_tables[used_name] = table_name
                elif dml_table_expression_clause_node.get_child_by_value('select_statement') is not None:
                    cte_names.append(cur_layer_tables)
                    subquery_node = dml_table_expression_clause_node.get_children_by_path(['select_statement',
                                                                                           'select_only_statement',
                                                                                           'subquery'])
                    assert len(subquery_node) == 1
                    subquery_node = subquery_node[0]
                    sub_tree_node = SubTreeNode(subquery_node, dialect=dialect, db_param=db_param)
                    simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(subquery_node)
                    for simple_select_node in simple_select_nodes:
                        analyze_subtree_nodes(simple_select_node, cte_names, dialect, sub_tree_node, db_param)
                    alias_node = table_ref_aux_node.get_child_by_value('table_alias')
                    if alias_node is not None:
                        name = str(alias_node).strip('"')
                        cur_layer_tables[name] = sub_tree_node
                    cte_names.remove(cur_layer_tables)
                    cur_subtree_node.add_child_node(sub_tree_node)
                elif dml_table_expression_clause_node.get_child_by_value('subquery') is not None:
                    subquery_node = dml_table_expression_clause_node.get_child_by_value('subquery')
                    if dml_table_expression_clause_node.get_child_by_value('LATERAL') is not None:
                        cte_names.append(cur_layer_tables)
                    sub_tree_node = SubTreeNode(subquery_node, dialect=dialect, db_param=db_param)
                    simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(subquery_node)
                    for simple_select_node in simple_select_nodes:
                        analyze_subtree_nodes(simple_select_node, cte_names, dialect, sub_tree_node, db_param)
                    if dml_table_expression_clause_node.get_child_by_value('LATERAL') is not None:
                        cte_names.remove(cur_layer_tables)
                    alias_node = table_ref_aux_node.get_child_by_value('table_alias')
                    if alias_node is not None:
                        name = str(alias_node).strip('"')
                        cur_layer_tables[name] = sub_tree_node
                    cur_subtree_node.add_child_node(sub_tree_node)
                else:
                    continue
    else:
        assert False


def build_subquery_tree(root_node: TreeNode, dialect: str, cte_nodes: dict, db_param) -> SubTreeNode:
    derived_tables = [cte_nodes]
    if dialect == 'mysql':
        select_statement_node = root_node.get_children_by_path(['sqlStatements', 'sqlStatement',
                                                                'dmlStatement', 'selectStatement'])
        assert len(select_statement_node) == 1
        select_statement_node = select_statement_node[0]
        select_stmts = fetch_all_simple_select_from_select_stmt_mysql(select_statement_node)
    elif dialect in ['pg', 'snowflake', 'sqlserver']:
        select_stmt_node = root_node.get_children_by_path(['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        select_stmts = fetch_all_simple_select_from_select_stmt_pg(select_stmt_node)
    elif dialect == 'oracle':
        select_stmt_node = root_node.get_children_by_path(['unit_statement', 'data_manipulation_language_statements',
                                                           'select_statement', 'select_only_statement'])
        assert len(select_stmt_node) == 1
        subquery_node = root_node.get_children_by_path(['unit_statement', 'data_manipulation_language_statements',
                                                        'select_statement', 'select_only_statement', 'subquery'])
        if len(subquery_node) != 1:
            print('FOR UPDATE haven\'t been supported yet')
            assert False
        select_stmt_node = subquery_node[0]
        select_stmts = fetch_all_simple_select_from_subquery_oracle(select_stmt_node)
    else:
        assert False
    root_sub_tree_node = SubTreeNode(root_node, dialect=dialect, db_param=db_param)
    for simple_select_node in select_stmts:
        analyze_subtree_nodes(simple_select_node, derived_tables, dialect, root_sub_tree_node, db_param)
    return root_sub_tree_node


def build_ctes(root_node: TreeNode, dialect: str, db_param):
    # use stack
    cte_nodes = {}
    if dialect == 'mysql':
        with_stmt_node = root_node.get_children_by_path(
            ['sqlStatements', 'sqlStatement', 'dmlStatement', 'withStatement'])
        if len(with_stmt_node) != 0:
            assert len(with_stmt_node) == 1
            with_stmt_node = with_stmt_node[0]
            common_table_expressions = with_stmt_node.get_children_by_value('commonTableExpression')
            for cte_root_node in common_table_expressions:
                cte_name = str(cte_root_node.get_child_by_value('cteName')).strip('`')
                query_body_node = cte_root_node.get_child_by_value('dmlStatement')
                select_stmt_node = query_body_node.get_child_by_value('selectStatement')
                select_stmts = fetch_all_simple_select_from_select_stmt_mysql(select_stmt_node)
                cte_sub_tree_node = SubTreeNode(query_body_node, dialect=dialect, db_param=db_param)
                cte_nodes[cte_name] = cte_sub_tree_node
                for select_stmt_node in select_stmts:
                    res = build_subquery_tree(select_stmt_node, dialect, cte_nodes, db_param)
                    cte_sub_tree_node.add_child_node(res)
                merge_subtree_nodes(cte_sub_tree_node)
    elif dialect in ['pg', 'snowflake', 'sqlserver']:
        select_stmt_node = root_node.get_children_by_path(['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        select_main_node = get_pg_main_select_node_from_select_stmt(select_stmt_node)
        with_clause_node = select_main_node.get_child_by_value('with_clause')
        if with_clause_node is not None:
            cte_nodes = with_clause_node.get_children_by_path(['cte_list', 'common_table_expr'])
            for cte_node in cte_nodes:
                cte_name = str(cte_node.get_child_by_value('name')).strip('"')
                query_body_node = cte_node.get_children_by_path(['preparablestmt', 'selectstmt'])
                assert len(query_body_node) == 1
                query_body_node = query_body_node[0]
                simple_select_nodes = fetch_all_simple_select_from_select_stmt_pg(query_body_node)
                cte_sub_tree_node = SubTreeNode(query_body_node, dialect=dialect, db_param=db_param)
                cte_nodes[cte_name] = cte_sub_tree_node
                for select_stmt_node in simple_select_nodes:
                    res = build_subquery_tree(select_stmt_node, dialect, cte_nodes, db_param)
                    cte_sub_tree_node.add_child_node(res)
                merge_subtree_nodes(cte_sub_tree_node)
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
                query_body_node = query_factoring_clause_node.get_child_by_value('subquery')
                cte_name = str(query_factoring_clause_node.get_child_by_value('query_name')).strip('"')
                simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(query_body_node)
                cte_sub_tree_node = SubTreeNode(query_body_node, dialect=dialect, db_param=db_param)
                cte_nodes[cte_name] = cte_sub_tree_node
                for select_stmt_node in simple_select_nodes:
                    res = build_subquery_tree(select_stmt_node, dialect, cte_nodes, db_param)
                    cte_sub_tree_node.add_child_node(res)
                merge_subtree_nodes(cte_sub_tree_node)
    else:
        assert False
    return cte_nodes


cnt = 0
cnt_lock = threading.Lock()


def global_id():
    global cnt
    with cnt_lock:
        cnt += 1
        return cnt


def check_filter_satisfied(ddls, dialect, db_params, sql, to_produce_data: list | None):
    db_name = 'sampledb' + str(global_id())
    for ddl in ddls:
        flag, res = sql_execute(dialect, db_name, ddl, db_params)
        if not flag:
            return False
    flag, res = sql_execute(dialect, db_name, sql, db_params)
    drop_schema(db_name, dialect)
    if not flag:
        return False
    else:
        if to_produce_data is None:
            return len(res) > 0
        else:
            return res == to_produce_data


def llm_gen_data(sql, ddls, src_dialect, db_params, to_produce_data: list | None, error_info, existing_data):
    target_output_desc = (
        f"The SQL execution must produce exactly this result: {to_produce_data}"
        if to_produce_data else
        "The SQL execution must produce a NON-EMPTY result set."
    )

    error_context = f"\nNote: The previous attempt failed with error: {error_info}. Please fix the data logic accordingly." if error_info else ""

    prompt = GEN_PROMPT.format(ddls=';\n'.join(ddls), sql=sql, target_output_desc=target_output_desc,
                               error_context=error_context, src_dialect=src_dialect,
                               db_params='' if len(db_params) == 0 else json.dumps(db_params, indent=2),
                               existing_data=json.dumps(existing_data, indent=2))
    model = init_model('deepseek-r1')
    ans = model.trans_func([], '', prompt)
    return ans

limit = load_db_config()
max_oracle_sql = limit['max_len_oracle_sql']
max_mysql_sql = limit['max_len_mysql_sql']
max_pg_sql = limit['max_len_pg_sql']


def tran_to_insert(new_data, dialect, table_schema):
    ins_sqls = []
    table_name = table_schema['table']
    print(f'insert into table {table_name}')
    not_insert_flag = False
    if dialect == 'oracle':
        insert_sql = f"INSERT ALL\n"
    else:
        insert_sql = f"INSERT INTO {get_table_col_name(table_name, dialect)} VALUES "
    row_count = 1
    for row in new_data:
        value_str = ''
        # columns_str = ''
        cnt = 0
        for col in table_schema['cols']:
            value = row[col['col_name']]
            value_rep = build_value(col['type'], value, dialect)
            if value_rep is None:
                continue
            if value_str != '':
                value_str = value_str + ', '
            value_str = value_str + value_rep
            cnt += 1
        if dialect == 'oracle':
            try_all_sql = f"INSERT ALL INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str}) SELECT 1 FROM dual;"
            if len(try_all_sql) > max_oracle_sql:
                ins_sql = f"INSERT INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str})"
                ins_sqls.append(ins_sql)
            else:
                if len(f"{insert_sql} INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str}) SELECT 1 FROM dual;") < max_oracle_sql:
                    insert_sql = f"{insert_sql} INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str})"
                else:
                    ins_sqls.append(insert_sql + ' SELECT 1 FROM dual')
                    insert_sql = f"INSERT ALL INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str})"
                not_insert_flag = True
        elif dialect == 'mysql' or dialect == 'pg':
            if not_insert_flag is False:
                new_insert_sql = f"{insert_sql} ({value_str})"
            else:
                new_insert_sql = f"{insert_sql}, ({value_str})"
            if len(new_insert_sql) < max_mysql_sql:
                insert_sql = new_insert_sql
            else:
                ins_sqls.append(insert_sql)
                insert_sql = f"INSERT INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str})"
            not_insert_flag = True
        elif dialect == 'sqlserver':
            if not_insert_flag is False:
                new_insert_sql = f"{insert_sql} ({value_str})"
                row_count = 1
            else:
                new_insert_sql = f"{insert_sql}, ({value_str})"
                row_count += 1
            if len(new_insert_sql) < max_mysql_sql and row_count < 1000:
                insert_sql = new_insert_sql
            else:
                ins_sqls.append(insert_sql)
                insert_sql = f"INSERT INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str})"
                row_count = 1
            not_insert_flag = True
        else:
            assert False
    if not_insert_flag:
        if dialect == 'oracle':
            insert_sql = insert_sql + ' SELECT 1 FROM dual'
        ins_sqls.append(insert_sql)
    return ins_sqls


def is_subquery(string: str):
    return string.startswith(MAGIC_SUBQUERY_TOKEN)


import copy


def store_data_to_table(new_data, db_data):
    for key, value in new_data.items():
        if not is_subquery(key):
            if key in db_data:
                db_data[key] = db_data[key] + value
            else:
                db_data[key] = value


import re
import json


def extract_json_from_markdown(text):
    pattern = r"```(?:json)?\s*(\{.*?\})\s*```"
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        json_str = match.group(1)
        try:
            data = json.loads(json_str)
            return data
        except json.JSONDecodeError as e:
            print(f"JSON Parse error: {e}")
            return None
    else:
        return None

def gen_sample_db(root_node: SubTreeNode, src_dialect, db_params, to_produce_data: list | None, retry_time, db_data):
    revised_sql = root_node.get_revised_sql()
    schema, ddls = root_node.get_schema()
    used_ddls = []
    for key, value in ddls.items():
        used_ddls = used_ddls + value
    error_info = None
    i = 0
    while i < retry_time:
        temp_db_data = copy.deepcopy(db_data)
        gen_flag = True
        try:
            used_ddls = []
            for key, value in ddls.items():
                used_ddls = used_ddls + value
            existing_data = root_node.get_existing_data(temp_db_data)
            new_data_str = llm_gen_data(revised_sql, used_ddls, src_dialect, db_params,
                                        to_produce_data, error_info,
                                        json.dumps(existing_data, indent=2))
            if new_data_str == 'null':
                return False, None
            new_data_json = extract_json_from_markdown(new_data_str)
            store_data_to_table(new_data_json, temp_db_data)
            insert_ddls = []
            for table in schema:
                insert_ddls = insert_ddls + tran_to_insert(new_data_json.get(table, []), src_dialect, schema[table])
            # insert_ddls = tran_to_insert(new_data_json, src_dialect, schema)
        except Exception as e:
            i += 1
            continue
        if check_filter_satisfied(used_ddls + insert_ddls, src_dialect, db_params, revised_sql, to_produce_data):
            for child in root_node.children:
                if not child.is_table():
                    flag, _ = gen_sample_db(child, src_dialect, db_params,
                                            to_produce_data[root_node.subquery_id_map[child]],
                                            retry_time, temp_db_data)
                    if not flag:
                        error_info = f"Fail to produce data for subquery: \n {str(child.tree_node)}"
                        gen_flag = False
                        break

            if gen_flag:
                db_data.clear()
                db_data.update(temp_db_data)
                return True, None
        i += 1
    return False, None


def fetch_all_table(root_node: SubTreeNode):
    table_set = set()
    if root_node.is_table():
        table_set.add(root_node.table_name)
    else:
        for child in root_node.children:
            table_set = table_set | fetch_all_table(child)
    return table_set


def gen_sample_db_for_sql(sql: str, src_dialect, retry_time, db_param):
    tree_node, _, _, _ = parse_tree(sql, src_dialect)
    if tree_node is None:
        return None
    tree_node = TreeNode.make_g4_tree_by_node(tree_node, src_dialect)
    cte_nodes = build_ctes(tree_node, src_dialect, db_param)
    root_node = build_subquery_tree(tree_node, src_dialect, cte_nodes, db_param)
    db_data = {}
    gen_sample_db(root_node, src_dialect, db_param, None, retry_time, db_data)
    sample_db_id = f'sample_db_{global_id()}'
    tables = fetch_all_table(root_node)
    db_ids = get_db_ids()
    ddls = []
    insert_ddls = []
    for db_id in db_ids:
        schema, add_constraints, type_defs = schema_build(db_id, src_dialect)
        for table in tables:
            if table in schema:
                create_stmt = create_table(schema[table], copy.deepcopy(add_constraints[table]), src_dialect)
                ddls = ddls + type_defs[table] + [create_stmt]
                insert_ddls = insert_ddls + tran_to_insert(db_data.get(table, {}), src_dialect, schema[table])
    for ddl in ddls + insert_ddls:
        flag, res = sql_execute(src_dialect, sample_db_id, ddl, db_param, False, False)
        if not flag:
            print(res)
            print(ddl)
            return None
    return sample_db_id



def sample_db_execution_verify(sql, res_sql, db_param, dialect, order_mode, retry_limit=2):
    db_id = gen_sample_db_for_sql(res_sql, dialect, retry_limit, db_param.get(dialect, {}))
    if db_id is None:
        return None
    flag1, res1 = sql_dependent_execute(dialect, db_id, sql, db_param.get(dialect, {}))
    flag2, res2 = sql_dependent_execute(dialect, db_id, res_sql, db_param.get(dialect, {}))
    drop_schema(db_id, dialect, False, False)
    if not flag1:
        return False, res1, res1, res2
    if not flag2:
        print(res2)
    assert flag2
    if len(res1) == 0 and len(res2) == 0:
        return True, 'No result', res1, res2
    if not order_mode:
        if tol_order_unaware_compare(res1, res2):
            return True, '', res1, res2
        else:
            return False, 'inconsistent_result', res1, res2
    else:
        if tol_order_aware_compare(res1, res2):
            return True, '', res1, res2
        else:
            return False, 'inconsistent_result', res1, res2
