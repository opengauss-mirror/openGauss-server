def check_enum(file_name, start, end, enum_prefix, multiple):
    enum_file = open(file_name, 'r')
    lines = enum_file.readlines()
    match_start = False
    last_value = -1
    skip = False
    for line in lines:
        if not match_start:
            if not line.startswith(start):
                continue
            else:
                match_start = True
        
        if not skip:
            if multiple and line.find('#ifndef ENABLE_MULTIPLE_NODES') > 0:
                skip = True
            elif not multiple and line.find('#ifdef ENABLE_MULTIPLE_NODES') > 0:
                skip = True

        if skip:
            if line.find('#endif') > 0:
                skip = False
            else:
                continue

        if line.startswith(enum_prefix):
            if line.find(' = ') > 0:
                tmp = last_value
                last_value = int(line.split(' = ')[1].split(',')[0])
                if last_value <= tmp:
                    err_msg = start + ' in "' + file_name + '" is not right at "' + line.replace('\n', '') + '"'
                    err_msg = err_msg + '\n' + 'the above value is ' + str(tmp)
                    raise Exception(err_msg)
            else:
                last_value += 1
            # print(line, last_value)



if __name__ == '__main__':
    check_enum('src/include/nodes/nodes.h', 'typedef enum NodeTag', '} NodeTag;', '    T_', False)
    check_enum('src/include/nodes/nodes.h', 'typedef enum NodeTag', '} NodeTag;', '    T_', True)