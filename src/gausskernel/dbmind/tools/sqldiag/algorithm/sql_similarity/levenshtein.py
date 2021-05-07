def distance(str1, str2):
    """
    func: calculate levenshtein distance between two strings.
    :param str1: string1
    :param str2: string2
    :return: distance
    """
    m, n = len(str1) + 1, len(str2) + 1
    matrix = [[0] * n for _ in range(m)]
    matrix[0][0] = 0
    for i in range(1, m):
        matrix[i][0] = matrix[i - 1][0] + 1
        for j in range(1, n):
            matrix[0][j] = matrix[0][j - 1] + 1
    for i in range(1, m):
        for j in range(1, n):
            if str1[i - 1] == str2[j - 1]:
                matrix[i][j] = matrix[i - 1][j - 1]
            else:
                matrix[i][j] = max(matrix[i - 1][j - 1], matrix[i - 1][j], matrix[i][j - 1]) + 1

    return matrix[m - 1][n - 1]
