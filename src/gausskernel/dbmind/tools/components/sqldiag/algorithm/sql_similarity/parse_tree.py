import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison, Parenthesis, Values
from sqlparse.tokens import Keyword, DML, DDL

ALPHA = 0.8


def token2value(tokens):
    value_list = []
    for token in tokens:
        if isinstance(token, IdentifierList):
            for item in token.tokens:
                value_list.append(item.value)
        elif isinstance(token, Identifier) or isinstance(token, Comparison) or isinstance(token, Values):
            value_list.append(token.value)
    return value_list


def build_child_nodes(tokens, root=None):
    if root is None:
        root = {}
    st_child = False
    sub_tokens = []
    sub_sql_count = 0
    for token in tokens:
        if token.ttype in [Keyword, DDL, DML] or isinstance(token, Where) or isinstance(token, Values) \
                or isinstance(token, Parenthesis):
            if st_child:
                root[child_key] = build_child_nodes(sub_tokens)
            if isinstance(token, Where):
                root['Where'] = []
                for item in token.tokens:
                    if isinstance(item, Comparison):
                        root['Where'].append(item.value)
                st_child = False
            elif isinstance(token, Values):
                root['Values'] = token.value
                st_child = False
            elif isinstance(token, Parenthesis):
                sub_sql_count = sub_sql_count + 1
                child_key = 'sub_sql' + str(sub_sql_count)
                root[child_key] = build_child_nodes(token.tokens)
                st_child = False
            else:
                st_child = True
                sub_tokens = []
                child_key = token.value.lower()
        else:
            sub_tokens.append(token)
    if st_child:
        root[child_key] = build_child_nodes(sub_tokens)
    else:
        root = token2value(tokens)

    return root


def build_tree(sql):
    parsed_sql = sqlparse.parse(sql)[0]
    parse_tree = dict()
    build_child_nodes(parsed_sql.tokens, parse_tree)

    return parse_tree


def compare_two_tree(tree1, tree2, total_score, cur_score):
    if isinstance(tree1, dict) and isinstance(tree2, dict):
        for i in range(len(tree1)):
            key = list(tree1.keys())[i]
            if key in tree2.keys():
                total_score[0] += cur_score
                compare_two_tree(tree1[key], tree2[key], total_score, cur_score * ALPHA)
    if isinstance(tree1, list) and isinstance(tree2, list):
        for value in tree1:
            if value in tree2:
                total_score[0] += cur_score


def distance(sql1, sql2):
    parse_tree1 = build_tree(sql1)
    parse_tree2 = build_tree(sql2)
    similarity = [0]
    compare_two_tree(parse_tree1, parse_tree2, similarity, 1)
    return similarity[0]
