#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include <regex.h>
#include <time.h>

PG_MODULE_MAGIC;  // 标记这个模块是 PostgreSQL 的可加载模块

// 邮箱校验函数
PG_FUNCTION_INFO_V1(validate_email);

Datum
validate_email(PG_FUNCTION_ARGS)
{
    text *input_text = PG_GETARG_TEXT_PP(0);
    char *email = text_to_cstring(input_text);
    
    regex_t regex;
    int result;
    const char *pattern = "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$";

    result = regcomp(&regex, pattern, REG_EXTENDED);
    if (result) {
        ereport(ERROR, (errmsg("Failed to compile regex")));
    }

    result = regexec(&regex, email, 0, NULL, 0);
    regfree(&regex);

    PG_RETURN_BOOL(result == 0);
}

