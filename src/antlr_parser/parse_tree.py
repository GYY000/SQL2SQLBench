import sys

from antlr4 import *
from antlr4.error.ErrorListener import ErrorListener
from pygments.lexers.sql import SqlLexer

from antlr_parser.pg_parser.PostgreSQLParser import PostgreSQLParser
from antlr_parser.pg_parser.PostgreSQLLexer import PostgreSQLLexer

from antlr_parser.mysql_parser.MySqlParser import MySqlParser
from antlr_parser.mysql_parser.MySqlLexer import MySqlLexer

from antlr_parser.oracle_parser.PlSqlParser import PlSqlParser
from antlr_parser.oracle_parser.PlSqlLexer import PlSqlLexer
from antlr_parser.snowflake_parser.SnowflakeLexer import SnowflakeLexer
from antlr_parser.snowflake_parser.SnowflakeParser import SnowflakeParser
from antlr_parser.sqlserver_parser.TSqlLexer import TSqlLexer
from antlr_parser.sqlserver_parser.TSqlParser import TSqlParser
from sql_gen.generator.point_type.TranPointType import TranPointType

map_parser = ["postgres", "mysql", "oracle"]


class CustomErrorListener(ErrorListener):
    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        raise SelfParseError(line, column, msg)


def parse_tree(src_sql: str, dialect: str) -> (str, int, int, str):
    if dialect in ['pg', 'snowflake', 'sqlserver']:
        return parse_pg_tree(src_sql)
    elif dialect == 'mysql':
        return parse_mysql_tree(src_sql)
    elif dialect == 'oracle':
        return parse_oracle_tree(src_sql)
    # elif dialect == 'snowflake':
    #     return parse_snowflake_tree(src_sql)
    # elif dialect == 'sqlserver':
    #     return parse_snowflake_tree(src_sql)
    else:
        raise ValueError("use one of" + str(map_parser) + " as argument")


def parse_element_tree(function_expr: str, dialect: str, point_type: TranPointType) -> (str, int, int, str):
    if dialect in ['pg', 'snowflake', 'sqlserver']:
        try:
            input_stream = InputStream(function_expr)
            lexer = PostgreSQLLexer(input_stream)
            lexer.addErrorListener(CustomErrorListener())
            stream = CommonTokenStream(lexer)
            parser = PostgreSQLParser(stream)
            parser.addErrorListener(CustomErrorListener())
            if not hasattr(parser, point_type.parsing_rule_name(dialect)):
                raise AttributeError(f"{parser.__class__.__name__} "
                                     f"has no method named {point_type.parsing_rule_name(dialect)}")
            method = getattr(parser, point_type.parsing_rule_name(dialect))
            tree = method()
            return tree, None, None, None
        except SelfParseError as e:
            return None, e.line, e.column, e.msg
        except Exception as e:
            print(f"An error occurred: {e}", file=sys.stderr)
            raise e
    elif dialect == 'mysql':
        try:
            input_stream = InputStream(function_expr)
            lexer = MySqlLexer(input_stream)
            lexer.addErrorListener(CustomErrorListener())
            stream = CommonTokenStream(lexer)
            parser = MySqlParser(stream)
            parser.addErrorListener(CustomErrorListener())
            if not hasattr(parser, point_type.parsing_rule_name(dialect)):
                raise AttributeError(f"{parser.__class__.__name__} "
                                     f"has no method named {point_type.parsing_rule_name(dialect)}")
            method = getattr(parser, point_type.parsing_rule_name(dialect))
            tree = method()
            return tree, None, None, None
        except SelfParseError as e:
            return None, e.line, e.column, e.msg
        except Exception as e:
            print(f"An error occurred: {e}", file=sys.stderr)
            raise e
    elif dialect == 'oracle':
        try:
            input_stream = InputStream(function_expr)
            lexer = PlSqlLexer(input_stream)
            lexer.addErrorListener(CustomErrorListener())
            stream = CommonTokenStream(lexer)
            parser = PlSqlParser(stream)
            parser.addErrorListener(CustomErrorListener())
            if not hasattr(parser, point_type.parsing_rule_name(dialect)):
                raise AttributeError(f"{parser.__class__.__name__} "
                                     f"has no method named {point_type.parsing_rule_name(dialect)}")
            method = getattr(parser, point_type.parsing_rule_name(dialect))
            tree = method()
            return tree, None, None, None
        except SelfParseError as e:
            return None, e.line, e.column, e.msg
        except Exception as e:
            print(f"An error occurred: {e}", file=sys.stderr)
            raise e
    # elif dialect == 'snowflake':
    #     try:
    #         input_stream = InputStream(function_expr)
    #         lexer = SnowflakeLexer(input_stream)
    #         lexer.addErrorListener(CustomErrorListener())
    #         stream = CommonTokenStream(lexer)
    #         parser = SnowflakeParser(stream)
    #         parser.addErrorListener(CustomErrorListener())
    #         if not hasattr(parser, point_type.parsing_rule_name(dialect)):
    #             raise AttributeError(f"{parser.__class__.__name__} "
    #                                  f"has no method named {point_type.parsing_rule_name(dialect)}")
    #         method = getattr(parser, point_type.parsing_rule_name(dialect))
    #         tree = method()
    #         return tree, None, None, None
    #     except SelfParseError as e:
    #         return None, e.line, e.column, e.msg
    #     except Exception as e:
    #         print(f"An error occurred: {e}", file=sys.stderr)
    #         raise e
    # elif dialect == 'sqlserver':
    #     try:
    #         input_stream = InputStream(function_expr)
    #         lexer = TSqlLexer(input_stream)
    #         lexer.addErrorListener(CustomErrorListener())
    #         stream = CommonTokenStream(lexer)
    #         parser = TSqlParser(stream)
    #         parser.addErrorListener(CustomErrorListener())
    #         if not hasattr(parser, point_type.parsing_rule_name(dialect)):
    #             raise AttributeError(f"{parser.__class__.__name__} "
    #                                  f"has no method named {point_type.parsing_rule_name(dialect)}")
    #         method = getattr(parser, point_type.parsing_rule_name(dialect))
    #         tree = method()
    #         return tree, None, None, None
    #     except SelfParseError as e:
    #         return None, e.line, e.column, e.msg
    #     except Exception as e:
    #         print(f"An error occurred: {e}", file=sys.stderr)
    #         raise e
    else:
        assert False


def parse_ele_based_on_name(function_expr: str, dialect: str, rule_name: str) -> (str, int, int, str):
    if dialect == 'pg':
        try:
            input_stream = InputStream(function_expr)
            lexer = PostgreSQLLexer(input_stream)
            lexer.addErrorListener(CustomErrorListener())
            stream = CommonTokenStream(lexer)
            parser = PostgreSQLParser(stream)
            parser.addErrorListener(CustomErrorListener())
            if not hasattr(parser, rule_name):
                raise AttributeError(f"{parser.__class__.__name__} "
                                     f"has no method named {rule_name}")
            method = getattr(parser, rule_name)
            tree = method()
            return tree, None, None, None
        except SelfParseError as e:
            return None, e.line, e.column, e.msg
        except Exception as e:
            print(f"An error occurred: {e}", file=sys.stderr)
            raise e
    elif dialect == 'mysql':
        try:
            input_stream = InputStream(function_expr)
            lexer = MySqlLexer(input_stream)
            lexer.addErrorListener(CustomErrorListener())
            stream = CommonTokenStream(lexer)
            parser = MySqlParser(stream)
            parser.addErrorListener(CustomErrorListener())
            if not hasattr(parser, rule_name):
                raise AttributeError(f"{parser.__class__.__name__} "
                                     f"has no method named {rule_name}")
            method = getattr(parser, rule_name)
            tree = method()
            return tree, None, None, None
        except SelfParseError as e:
            return None, e.line, e.column, e.msg
        except Exception as e:
            print(f"An error occurred: {e}", file=sys.stderr)
            raise e
    elif dialect == 'oracle':
        try:
            input_stream = InputStream(function_expr)
            lexer = PlSqlLexer(input_stream)
            lexer.addErrorListener(CustomErrorListener())
            stream = CommonTokenStream(lexer)
            parser = PlSqlParser(stream)
            parser.addErrorListener(CustomErrorListener())
            if not hasattr(parser, rule_name):
                raise AttributeError(f"{parser.__class__.__name__} "
                                     f"has no method named {rule_name}")
            method = getattr(parser, rule_name)
            tree = method()
            return tree, None, None, None
        except SelfParseError as e:
            return None, e.line, e.column, e.msg
        except Exception as e:
            print(f"An error occurred: {e}", file=sys.stderr)
            raise e
    elif dialect == 'snowflake':
        try:
            input_stream = InputStream(function_expr)
            lexer = SnowflakeLexer(input_stream)
            lexer.addErrorListener(CustomErrorListener())
            stream = CommonTokenStream(lexer)
            parser = SnowflakeParser(stream)
            parser.addErrorListener(CustomErrorListener())
            if not hasattr(parser, rule_name):
                raise AttributeError(f"{parser.__class__.__name__} "
                                     f"has no method named {rule_name}")
            method = getattr(parser, rule_name)
            tree = method()
            return tree, None, None, None
        except SelfParseError as e:
            return None, e.line, e.column, e.msg
        except Exception as e:
            print(f"An error occurred: {e}", file=sys.stderr)
            raise e
    elif dialect == 'sqlserver':
        try:
            input_stream = InputStream(function_expr)
            lexer = TSqlLexer(input_stream)
            lexer.addErrorListener(CustomErrorListener())
            stream = CommonTokenStream(lexer)
            parser = TSqlParser(stream)
            parser.addErrorListener(CustomErrorListener())
            if not hasattr(parser, rule_name):
                raise AttributeError(f"{parser.__class__.__name__} "
                                     f"has no method named {rule_name}")
            method = getattr(parser, rule_name)
            tree = method()
            return tree, None, None, None
        except SelfParseError as e:
            return None, e.line, e.column, e.msg
        except Exception as e:
            print(f"An error occurred: {e}", file=sys.stderr)
            raise e
    else:
        assert False


def parse_pg_tree(src_sql: str) -> (str, int, int, str):
    try:
        input_stream = InputStream(src_sql)
        lexer = PostgreSQLLexer(input_stream)
        lexer.addErrorListener(CustomErrorListener())
        stream = CommonTokenStream(lexer)
        parser = PostgreSQLParser(stream)
        parser.addErrorListener(CustomErrorListener())
        tree = parser.root()
        # print(tree.toStringTree(recog=parser))
        return tree, None, None, None
    except SelfParseError as e:
        return None, e.line, e.column, e.msg
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        raise e


def parse_mysql_tree(src_sql: str):
    try:
        input_stream = InputStream(src_sql)
        lexer = MySqlLexer(input_stream)
        lexer.addErrorListener(CustomErrorListener())
        stream = CommonTokenStream(lexer)
        parser = MySqlParser(stream)
        parser.addErrorListener(CustomErrorListener())
        tree = parser.root()
        # print(tree.toStringTree(recog=parser))
        return tree, None, None, None
    except SelfParseError as e:
        return None, e.line, e.column, e.msg
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        raise e


def parse_oracle_tree(src_sql: str):
    try:
        input_stream = InputStream(src_sql)
        lexer = PlSqlLexer(input_stream)
        lexer.addErrorListener(CustomErrorListener())
        stream = CommonTokenStream(lexer)
        parser = PlSqlParser(stream)
        parser.addErrorListener(CustomErrorListener())
        tree = parser.sql_script()
        # print(tree.toStringTree(recog=parser))
        return tree, None, None, None
    except SelfParseError as e:
        return None, e.line, e.column, e.msg
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        raise e


def parse_snowflake_tree(src_sql: str):
    try:
        input_stream = InputStream(src_sql)
        lexer = SnowflakeLexer(input_stream)
        lexer.addErrorListener(CustomErrorListener())
        stream = CommonTokenStream(lexer)
        parser = SnowflakeParser(stream)
        parser.addErrorListener(CustomErrorListener())
        tree = parser.snowflake_file()
        return tree, None, None, None
    except SelfParseError as e:
        return None, e.line, e.column, e.msg
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        raise e


def parse_sqlserver_tree(src_sql: str):
    try:
        input_stream = InputStream(src_sql)
        lexer = TSqlLexer(input_stream)
        lexer.addErrorListener(CustomErrorListener())
        stream = CommonTokenStream(lexer)
        parser = TSqlParser(stream)
        parser.addErrorListener(CustomErrorListener())
        tree = parser.tsql_file()
        return tree, None, None, None
    except SelfParseError as e:
        return None, e.line, e.column, e.msg
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        return None, -1, -1, ''



def get_lexer_parser(dialect: str):
    input_stream = InputStream('')
    if dialect in ['pg', 'snowflake', 'sqlserver']:
        lexer = PostgreSQLLexer(input_stream)
        stream = CommonTokenStream(lexer)
        return lexer, PostgreSQLParser(stream)
    elif dialect == 'mysql':
        lexer = MySqlLexer(input_stream)
        stream = CommonTokenStream(lexer)
        return lexer, MySqlParser(stream)
    elif dialect == 'oracle':
        lexer = PlSqlLexer(input_stream)
        stream = CommonTokenStream(lexer)
        return lexer, PlSqlParser(stream)
    # elif dialect == 'snowflake':
    #     lexer = SnowflakeLexer(input_stream)
    #     stream = CommonTokenStream(lexer)
    #     return lexer, SnowflakeParser(stream)
    # elif dialect == 'sqlserver':
    #     lexer = TSqlLexer(input_stream)
    #     stream = CommonTokenStream(lexer)
    #     return lexer, TSqlParser(stream)
    else:
        raise ValueError(f"Only support {map_parser}")


class SelfParseError(Exception):
    def __init__(self, line, column, msg):
        super().__init__(f"Syntax error at line {line} , column {column} : {msg}")
        self.line = line
        self.column = column
        self.msg = msg
