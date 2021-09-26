import pglast
from pglast import parsea_sql, Node


def get_from_select(sql):
    root = Node(parse_sql(sql))
    if root[0].stmt.node_tag == 'SelectStmt':
        pass