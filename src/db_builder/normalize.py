# -*- coding: utf-8 -*-
# @Project: LLM4DB
# @Module: normalize$
# @Author: 10379
# @Time: 2024/10/6 22:19
from typing import List

from antlr_parser.general_tree_analysis import in_oracle_column_table_name_path, in_pg_column_table_name_path, \
    in_mysql_column_table_name_path, father_value_list_compare
from antlr_parser.parse_tree import parse_tree
from antlr_parser.Tree import TreeNode
from utils.tools import get_used_reserved_keyword_list


def remove_as_mysql(root_node: TreeNode):
    remove_children = []
    if root_node.value in ['tableSourceItem', 'selectElement']:
        for child in root_node.children:
            if child.is_terminal and child.value.upper() == 'AS':
                remove_children.append(child)
        for child in remove_children:
            root_node.children.remove(child)
    for child in root_node.children:
        if not child.is_terminal:
            remove_as_mysql(child)


def remove_as_pg(root_node: TreeNode):
    remove_children = []
    if root_node.value in ['target_el', 'table_alias_clause']:
        for child in root_node.children:
            if child.is_terminal and child.value.upper() == 'AS':
                remove_children.append(child)
        for child in remove_children:
            root_node.children.remove(child)
    for child in root_node.children:
        if not child.is_terminal:
            remove_as_pg(child)


def add_quote_mysql(root_node: TreeNode, quote_type: str, lower_case_flag: bool):
    if in_mysql_column_table_name_path(root_node):
        add_quote_to_bot_node(root_node, quote_type, lower_case_flag)
    elif father_value_list_compare(root_node, ['dottedId', 'fullColumnName']):
        if root_node.is_terminal and not root_node.value == '.':
            assert root_node.value[0] == '.'
            src_str = root_node.value[1:]
            res = add_quote(src_str, quote_type, lower_case_flag)
            root_node.value = '.' + res
        elif root_node.is_terminal and root_node.value == '.':
            pass
        else:
            assert root_node.value == 'uid'
            add_quote_to_bot_node(root_node, quote_type, lower_case_flag)
    else:
        for child in root_node.children:
            add_quote_mysql(child, quote_type, lower_case_flag)


def remove_quote_mysql(root_node: TreeNode):
    if in_mysql_column_table_name_path(root_node):
        rm_quote_to_bot_node(root_node, '`')
    elif father_value_list_compare(root_node, ['dottedId', 'fullColumnName']):
        if root_node.is_terminal and not root_node.value == '.':
            assert root_node.value[0] == '.'
            src_str = root_node.value[1:]
            res = rm_quote(src_str, '`')
            root_node.value = '.' + res
        elif root_node.is_terminal and root_node.value == '.':
            pass
        else:
            assert root_node.value == 'uid'
            rm_quote_to_bot_node(root_node, '`')
    else:
        for child in root_node.children:
            remove_quote_mysql(child)


def add_quote_oracle(root_node: TreeNode, quote_type: str, lower_case_flag: bool):
    if in_oracle_column_table_name_path(root_node):
        add_quote_to_bot_node(root_node, quote_type, lower_case_flag)
    else:
        for child in root_node.children:
            add_quote_oracle(child, quote_type, lower_case_flag)


def remove_quote_oracle(root_node: TreeNode):
    if in_oracle_column_table_name_path(root_node):
        rm_quote_to_bot_node(root_node, '"')
    else:
        for child in root_node.children:
            remove_quote_oracle(child)


def add_quote_pg(root_node: TreeNode, quote_type, lower_case_flag: bool):
    if in_pg_column_table_name_path(root_node):
        add_quote_to_bot_node(root_node, quote_type, lower_case_flag)
    else:
        for child in root_node.children:
            add_quote_pg(child, quote_type, lower_case_flag)


def remove_quote_pg(root_node: TreeNode):
    if in_pg_column_table_name_path(root_node):
        rm_quote_to_bot_node(root_node, '"')
    else:
        for child in root_node.children:
            remove_quote_pg(child)


def rm_quote_to_bot_node(root_node: TreeNode, quote_type: str):
    while not root_node.is_terminal:
        root_node = root_node.children[0]
    root_node.value = rm_quote(root_node.value, quote_type)


def rm_quote(src_str: str, quote_type: str) -> str:
    if src_str.startswith(quote_type):
        assert len(src_str) > 0
        src_str = src_str[1:]
    if src_str.endswith(quote_type):
        src_str = src_str[:-1]
    return src_str


def add_quote(src_str: str, quote_type: str, lower_case_flag: bool) -> str:
    if src_str.startswith('\''):
        return src_str
    if quote_type == '\"':
        reverse_quote = '`'
    else:
        reverse_quote = '\"'
    res = ''
    if src_str.startswith(reverse_quote):
        assert len(src_str) > 0
        src_str = src_str[1:]
    if src_str.endswith(reverse_quote):
        src_str = src_str[:-1]

    if not src_str.startswith(quote_type):
        res = res + quote_type
    res = res + src_str
    if not src_str.endswith(quote_type):
        res = res + quote_type
    if not lower_case_flag:
        return res
    else:
        return res.lower()


def add_quote_to_bot_node(root_node: TreeNode, quote_type: str, lower_flag: bool):
    while not root_node.is_terminal:
        root_node = root_node.children[0]
    root_node.value = add_quote(root_node.value, quote_type, lower_flag)


def normalize(root_node: TreeNode, src_dialect: str, tgt_dialect: str):
    if tgt_dialect == 'oracle':
        if src_dialect == 'mysql':
            remove_as_mysql(root_node)
            add_quote_mysql(root_node, '\"', True)
        elif src_dialect == 'pg':
            remove_as_pg(root_node)
            add_quote_pg(root_node, '\"', True)
        elif src_dialect == 'oracle':
            add_quote_oracle(root_node, '\"', True)
    elif tgt_dialect == 'mysql':
        if src_dialect == 'oracle':
            add_quote_oracle(root_node, '`', True)
        elif src_dialect == 'pg':
            add_quote_pg(root_node, '`', True)
        elif src_dialect == 'mysql':
            add_quote_mysql(root_node, '`', True)
    elif tgt_dialect == 'pg':
        if src_dialect == 'oracle':
            add_quote_oracle(root_node, '\"', True)
        elif src_dialect == 'mysql':
            add_quote_mysql(root_node, '\"', True)
        elif src_dialect == 'pg':
            add_quote_pg(root_node, '\"', True)


def normalize_sql(sql: str, src_dialect: str, tgt_dialect: str):
    tree_node, _, _, _ = parse_tree(sql, src_dialect)
    if tree_node is not None:
        node = TreeNode.make_g4_tree_by_node(tree_node, src_dialect)
        normalize(node, src_dialect, tgt_dialect)
        return node
    else:
        return None


def remove_sql_quote(sql, dialect):
    tree_node, _, _, _ = parse_tree(sql, dialect)
    if tree_node is not None:
        node = TreeNode.make_g4_tree_by_node(tree_node, dialect)
        if dialect == 'mysql':
            remove_quote_mysql(node)
        elif dialect == 'pg':
            remove_quote_pg(node)
        elif dialect == 'oracle':
            remove_quote_oracle(node)
        return str(node)
    else:
        return None


def remove_for_oracle(sql, dialect):
    tree_node, _, _, _ = parse_tree(sql, dialect)
    if tree_node is not None:
        node = TreeNode.make_g4_tree_by_node(tree_node, dialect)
        if dialect == 'mysql':
            remove_as_mysql(node)
        elif dialect == 'pg':
            remove_as_pg(node)
        return str(node)
    else:
        return None


def normalize_specific_sql(sql, dialect):
    return str(normalize_sql(sql, dialect, dialect))


def rep_quote(src_str: str, reserved_tgt_dialect: List[str], quote_type: str) -> str:
    if quote_type == '[':
        front_quote = quote_type
        back_quote = ']'
    else:
        front_quote = quote_type
        back_quote = quote_type
    if src_str.startswith('\''):
        return src_str
    real_name = src_str.strip().strip('`').strip('"').strip('[').strip(']')
    flag = True
    for char in real_name:
        if not (char.isalnum() or char == '_'):
            flag = False
    if real_name.upper() in reserved_tgt_dialect or not flag:
        used_name = front_quote + real_name + back_quote
    else:
        used_name = real_name
    return used_name.lower()


def rep_quote_to_bot_node(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    while not root_node.is_terminal:
        root_node = root_node.children[0]
    root_node.value = rep_quote(root_node.value, reserved_tgt_dialect, quote_type)


def rep_quote_mysql(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    if in_mysql_column_table_name_path(root_node):
        rep_quote_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    elif father_value_list_compare(root_node, ['dottedId', 'fullColumnName']):
        if root_node.is_terminal and not root_node.value == '.':
            assert root_node.value[0] == '.'
            src_str = root_node.value[1:]
            res = rep_quote(src_str, reserved_tgt_dialect, quote_type)
            root_node.value = '.' + res
        elif root_node.is_terminal and root_node.value == '.':
            pass
        else:
            assert root_node.value == 'uid'
            rep_quote_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    else:
        for child in root_node.children:
            rep_quote_mysql(child, reserved_tgt_dialect, quote_type)


def rep_quote_pg(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    if in_pg_column_table_name_path(root_node):
        rep_quote_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    else:
        for child in root_node.children:
            rep_quote_pg(child, reserved_tgt_dialect, quote_type)


def rep_quote_oracle(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    if in_oracle_column_table_name_path(root_node):
        rep_quote_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    else:
        for child in root_node.children:
            rep_quote_oracle(child, reserved_tgt_dialect, quote_type)


def rep_reserved_keyword_quote(sql: str | None, tree_node: TreeNode | None, src_dialect, tgt_dialect) -> str | None:
    assert not (sql is None and tree_node is None)
    if tree_node is None:
        tree_node, _, _, _ = parse_tree(sql, src_dialect)
        if tree_node is None:
            return None
        tree_node = TreeNode.make_g4_tree_by_node(tree_node, src_dialect)

    reserved_keywords = get_used_reserved_keyword_list()
    tgt_reserved_keyword_list = reserved_keywords[tgt_dialect]
    if tgt_dialect in ['oracle', 'pg', 'snowflake']:
        quote_type = '"'
    elif tgt_dialect == 'mysql':
        quote_type = '`'
    elif tgt_dialect == 'sqlserver':
        quote_type = '['
    else:
        assert False
    if src_dialect == 'mysql':
        rep_quote_mysql(tree_node, tgt_reserved_keyword_list, quote_type)
    elif src_dialect in ['pg', 'snowflake', 'sqlserver']:
        rep_quote_pg(tree_node, tgt_reserved_keyword_list, quote_type)
    elif src_dialect == 'oracle':
        rep_quote_oracle(tree_node, tgt_reserved_keyword_list, quote_type)
    else:
        assert False
    return str(tree_node)


def rm_quote_reserved(src_str: str, reserved_word_list: List[str], quote_type: str) -> str:
    if src_str.startswith('\''):
        return src_str
    real_name = src_str.strip().strip('`').strip('"').strip('[').strip(']')
    if real_name.upper() in reserved_word_list:
        used_name = real_name
    else:
        used_name = src_str
    return used_name.lower()


def rm_quote_reserved_to_bot_node(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    while not root_node.is_terminal:
        root_node = root_node.children[0]
    root_node.value = rm_quote_reserved(root_node.value, reserved_tgt_dialect, quote_type)


def rm_quote_mysql(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    if in_mysql_column_table_name_path(root_node):
        rm_quote_reserved_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    elif father_value_list_compare(root_node, ['dottedId', 'fullColumnName']):
        if root_node.is_terminal and not root_node.value == '.':
            assert root_node.value[0] == '.'
            src_str = root_node.value[1:]
            res = rm_quote_reserved(src_str, reserved_tgt_dialect, quote_type)
            root_node.value = '.' + res
        elif root_node.is_terminal and root_node.value == '.':
            pass
        else:
            assert root_node.value == 'uid'
            rm_quote_reserved_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    else:
        for child in root_node.children:
            rm_quote_mysql(child, reserved_tgt_dialect, quote_type)


def rm_quote_pg(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    if in_pg_column_table_name_path(root_node):
        rm_quote_reserved_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    else:
        for child in root_node.children:
            rm_quote_pg(child, reserved_tgt_dialect, quote_type)


def rm_quote_oracle(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    if in_oracle_column_table_name_path(root_node):
        rm_quote_reserved_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    else:
        for child in root_node.children:
            rm_quote_oracle(child, reserved_tgt_dialect, quote_type)


def rm_sql_quote_reserved(tree_node: TreeNode, dialect: str, rm_word: list[str]):
    if dialect == 'oracle':
        quote_type = '"'
    elif dialect == 'mysql':
        quote_type = '`'
    elif dialect == 'pg':
        quote_type = '"'
    elif dialect == 'snowflake':
        quote_type = '"'
    elif dialect == 'sqlserver':
        quote_type = '['
    else:
        assert False
    if dialect == 'mysql':
        rm_quote_mysql(tree_node, rm_word, quote_type)
    elif dialect in ['pg', 'sqlserver', 'snowflake']:
        rm_quote_pg(tree_node, rm_word, quote_type)
    elif dialect == 'oracle':
        rm_quote_oracle(tree_node, rm_word, quote_type)
    else:
        assert False
    return str(tree_node)
