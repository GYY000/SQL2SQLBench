from db_builder.schema_builder import schema_build, create_table
from sql_gen.generator.point_loader import load_point_by_name
from utils.tools import get_db_ids


def fetch_db_param(point_name, src_dialect, tgt_dialect):
    try:
        point = load_point_by_name(src_dialect, tgt_dialect, point_name)
        if "Tag" in point:
            param = point['Tag']
            if 'DB PARAMETER' in param:
                return param['DB PARAMETER']
        return {}
    except Exception as e:
        return {}


def create_stmt_fetch(tables: list[str], dialect: str):
    # fetch the sub-graph in db
    db_ids = get_db_ids()
    create_statements = []
    for db_id in db_ids:
        schema, add_constraints, type_defs = schema_build(db_id, dialect)
        for table in tables:
            if table in schema:
                create_table_stmt = create_table(schema[table], add_constraints[table], dialect)
                create_statements.append(create_table_stmt)
                create_statements = create_statements + type_defs[table]
    return create_statements
