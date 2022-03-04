def distance(str1, str2):
    sql_distance = 0.0
    list1 = str1.split()
    list2 = str2.split()
    sorted_list1 = sorted(list1)
    sorted_list2 = sorted(list2)
    max_len = max(len(sorted_list1), len(sorted_list2))
    min_len = min(len(sorted_list1), len(sorted_list2))
    short_list = sorted_list1 if len(sorted_list1) < len(sorted_list2) else sorted_list2
    long_list = sorted_list1 if len(sorted_list1) > len(sorted_list2) else sorted_list2
    for item in short_list:
        if item in long_list:
            sql_distance += 1.0
    length_similarity = float(min_len / max_len)
    return sql_distance + length_similarity
