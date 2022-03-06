def calc_sql_distance(algorithm):
    if algorithm == 'list':
        from .list_distance import distance
    elif algorithm == 'levenshtein':
        from .levenshtein import distance
    elif algorithm == 'parse_tree':
        from .parse_tree import distance
    elif algorithm == 'cosine_distance':
        from .cosine_distance import distance
    else:
        raise NotImplementedError("do not support '{}'".format(algorithm))
    return distance
