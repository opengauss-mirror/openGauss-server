/* ---------------------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996, 2003 VIA Networking Technologies, Inc.
 *
 *  dfs_query_check.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/dfs/dfs_query_check.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "access/obs/obs_am.h"
#include "access/dfs/dfs_common.h"
#include "access/dfs/dfs_query.h"
#include "access/dfs/dfs_wrapper.h"
#include "catalog/pg_type.h"
#include "foreign/foreign.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"

#define NUMERIC_MAX_BITS 38
extern void VerifyEncoding(int encoding);
/*
 * @Description: Check the foldername validity for the OBS foreign table.
 * The first char and end char must be a '/'. Support the multi-foldername
 * for the OBS foreign table.
 * @in foldername, the given foldername.
 * @printName, the option name to be printed.
 * @delimiter, the string delimiter.
 * @return None.
 */
void checkObsPath(char *foldername, char *printName, const char *delimiter)
{
    if (0 == strlen(foldername)) {
        if (0 == pg_strcasecmp(printName, OPTION_NAME_FOLDERNAME)) {
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                            errmsg("No %s is specified for the foreign table.", printName)));
        } else {
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                            errmsg("Unsupport any empy %s for the foreign table.", printName)));
        }
    }

    char *tmpStr = NULL;
    char *separaterStr = NULL;
    char *tmp_token = NULL;
    errno_t rc = 0;
    size_t tmpStrLen = strlen(foldername);
    if (*foldername != '/' || foldername[tmpStrLen - 1] != '/') {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmodule(MOD_DFS),
                        errmsg("The first character and the end character of each %s"
                               " must be a '/' in string \"%s\".",
                               printName, foldername)));
    }

    tmpStr = (char *)palloc0(tmpStrLen + 1);
    rc = strncpy_s(tmpStr, tmpStrLen + 1, foldername, tmpStrLen);
    securec_check(rc, "\0", "\0");

    char *bucket = NULL;
    char *prefix = NULL;

    separaterStr = strtok_r(tmpStr, delimiter, &tmp_token);
    while (separaterStr != NULL) {
        char *tmp = separaterStr;
        /* detele ' ' before path. */
        while (*tmp == ' ') {
            tmp++;
        }

        if (strlen(tmp) == 0) {
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                            errmsg("Unsupport any empy %s for the foreign table.", printName)));
        }

        if (*tmp != '/' || tmp[strlen(tmp) - 1] != '/') {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmodule(MOD_DFS),
                            errmsg("The first character and the end character of each %s"
                                   " must be a '/' in string \"%s\".",
                                   printName, foldername)));
        }
        if (strlen(tmp) <= 2) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmodule(MOD_DFS),
                            errmsg("Unsupport any empy %s for the foreign table.", printName)));
        }

        /* check bucket or prefix is vaild. */
        FetchUrlPropertiesForQuery(tmp, &bucket, &prefix);
        Assert(bucket && prefix);

        pfree_ext(bucket);
        pfree_ext(prefix);

        separaterStr = strtok_r(NULL, delimiter, &tmp_token);
    }
    pfree_ext(tmpStr);
}

void CheckFoldernameOrFilenamesOrCfgPtah(const char *OptStr, char *OptType)
{
    const char *Errorchar = NULL;
    char BeginChar;
    char EndChar;
    uint32 i = 0;

    Assert(OptStr != NULL);
    Assert(OptType != NULL);

    /* description: remove the hdfs info. */
    if (strlen(OptStr) == 0) {
        if (pg_strncasecmp(OptType, OPTION_NAME_FOLDERNAME, NAMEDATALEN) == 0) {
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                            errmsg("No folder path is specified for the foreign table.")));
        } else if (pg_strncasecmp(OptType, OPTION_NAME_FILENAMES, NAMEDATALEN) == 0) {
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                            errmsg("No file path is specified for the foreign table.")));
        } else {
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                            errmsg("No hdfscfg path is specified for the server.")));
        }
    }
    size_t optStrLen = strlen(OptStr);
    for (i = 0; i < optStrLen; i++) {
        if (OptStr[i] == ' ') {
            if (i == 0 || (i - 1 > 0 && OptStr[i - 1] != '\\')) {
                ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                                errmsg("There is an illegal character \'%c\' in the option %s.", OptStr[i], OptType)));
            }
        }
    }
    BeginChar = *OptStr;
    EndChar = *(OptStr + strlen(OptStr) - 1);
    if (BeginChar == ',' || EndChar == ',') {
        ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                        errmsg("There is an illegal character \'%c\' in the option %s.", ',', OptType)));
    }
    if (0 == pg_strcasecmp(OptType, OPTION_NAME_FILENAMES) && EndChar == '/') {
        ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                        errmsg("The option %s should not be end with \'%c\'.", OptType, EndChar)));
    }

    Errorchar = strstr(OptStr, ",");
    if (Errorchar && 0 == pg_strcasecmp(OptType, OPTION_NAME_FOLDERNAME)) {
        ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                        errmsg("Only a folder path is allowed for the foreign table.")));
    }
    if (Errorchar && 0 == pg_strcasecmp(OptType, OPTION_NAME_CFGPATH)) {
        ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                        errmsg("Only a hdfscfg path is allowed for the server.")));
    }

    /*
     * The path must be an absolute path.
     */
    if (!is_absolute_path(OptStr)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmodule(MOD_DFS),
                        errmsg("The path \"%s\" must be an absolute path.", OptStr)));
    }
}

void CheckGetServerIpAndPort(const char *Address, List **AddrList, bool IsCheck, int real_addr_max)
{
    char *Str = NULL;
    char *Delimiter = NULL;
    char *SeparaterStr = NULL;
    char *tmp_token = NULL;
    HdfsServerAddress *ServerAddress = NULL;
    int addressCounter = 0;
    int addressMaxNum = (real_addr_max == -1 ? 2 : real_addr_max);
    errno_t rc = 0;

    Assert(Address != NULL);
    Str = (char *)palloc0(strlen(Address) + 1);
    rc = strncpy_s(Str, strlen(Address) + 1, Address, strlen(Address));
    securec_check(rc, "\0", "\0");

    /* Frist, check address stirng, the ' ' could not exist */
    if (strstr(Str, " ") != NULL) {
        ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                        errmsg("The address option exists illegal character: \'%c\'", ' ')));
    }

    if (strlen(Str) == 0) {
        ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                        errmsg("No address is specified for the server.")));
    }

    /* Check the address string, the first and last character could not be a character ',' */
    if (Str[strlen(Str) - 1] == ',' || *Str == ',') {
        ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                        errmsg("The address option exists illegal character: \'%c\'", ',')));
    }

    /* Now, we obtain ip string and port string */
    /* Separater Str use a ',' delimiter, for example xx.xx.xx.xx:xxxx,xx.xx.xx.xx:xxxx */
    Delimiter = ",";
    SeparaterStr = strtok_r(Str, Delimiter, &tmp_token);
    while (SeparaterStr != NULL) {
        char *AddrPort = NULL;
        int PortLen = 0;

        if (++addressCounter > addressMaxNum) {
            ereport(ERROR,
                    (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                     errmsg("The count of address \"%s\" must be not greater than %d.", Address, addressMaxNum)));
        }

        /* Judge ipv6 format or ipv4 format,like fe80::7888:bf24:e381:27:25000 */
        if (strstr(SeparaterStr, "::") != NULL) {
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                            errmsg("Unsupport ipv6 foramt")));
        } else if ((AddrPort = strstr(SeparaterStr, ":")) != NULL) {
            /* Deal with ipv4 format, like xx.xx.xx.xx:xxxx
             * Get SeparaterStr is "xx.xx.xx.xx" and AddrPort is xxxx.
             * Because the original SeparaterStr transform "xx.xx.xx.xx\0xxxxx"
             */
            *AddrPort++ = '\0';
        } else {
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                            errmsg("The incorrect address format")));
        }

        /* Check ip validity  */
        (void)DirectFunctionCall1(inet_in, CStringGetDatum(SeparaterStr));

        /* Check port validity */
        if (AddrPort != NULL) {
            PortLen = strlen(AddrPort);
        }
        if (PortLen != 0) {
            char *PortStr = AddrPort;
            while (*PortStr) {
                if (isdigit(*PortStr)) {
                    PortStr++;
                } else {
                    ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                                    errmsg("The address option exists illegal character: \'%c\'", *PortStr)));
                }
            }
            int portVal = pg_strtoint32(AddrPort);
            if (portVal > 65535) {
                ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                                errmsg("The port value is out of range: \'%s\'", AddrPort)));
            }
        } else {
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmodule(MOD_DFS),
                            errmsg("The incorrect address format")));
        }

        /* If IsCheck is false, get port and ip, Otherwise, only check validity of ip and port. */
        if (!IsCheck) {
            ServerAddress = (HdfsServerAddress *)palloc0(sizeof(HdfsServerAddress));
            ServerAddress->HdfsIp = SeparaterStr;
            ServerAddress->HdfsPort = AddrPort;
            *AddrList = lappend(*AddrList, ServerAddress);
        }

        SeparaterStr = strtok_r(NULL, Delimiter, &tmp_token);
    }
}

/*
 * brief: Decimal/numeric[p(,s)] data type is supported yet, but the max accuracy of decimal/numeric
 *        is less then 39 bits. So we must check it. If the decimal accuracy do not found in the
 *        TypeName struct, we will set NUMERICMAXBITS bit as default accuracy.
 * input param @TypName: the column typename struct.
 */
static void CheckNumericAccuracy(TypeName *TypName)
{
    Assert(TypName != NULL);
    if (TypName->typmods != NULL) {
        int32 precision;
        ListCell *typmods = list_head(TypName->typmods);
        Node *tmn = NULL;
        A_Const *ac = NULL;

        if (typmods == NULL || lfirst(typmods) == NULL) {
            return;
        }

        tmn = (Node *)lfirst(typmods);

        Assert(IsA(tmn, A_Const));

        ac = (A_Const *)tmn;
        precision = ac->val.val.ival;

        if (precision > NUMERIC_MAX_BITS) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                            errmsg("The maximum accuracy of decimal/numeric data type supported "
                                   "is %d bits.",
                                   NUMERIC_MAX_BITS)));
        }
    } else if (TypName->typemod != -1) {  // for create table like case
        uint32 typmod = (uint32)(TypName->typemod - VARHDRSZ);
        uint32 precision = (typmod >> 16) & 0xffff;
        if (precision > NUMERIC_MAX_BITS) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                            errmsg("The maximum accuracy of decimal/numeric data type supported "
                                   "is %d bits.",
                                   NUMERIC_MAX_BITS)));
        }
    } else {
        A_Const *n = makeNode(A_Const);
        n->val.type = T_Integer;
        n->val.val.ival = NUMERIC_MAX_BITS;
        n->location = -1;

        TypName->typmods = list_make1(n);
    }
}

void checkEncoding(DefElem *optionDef)
{
    int encoding = pg_char_to_encoding(defGetString(optionDef));
    if (encoding < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_DFS),
                        errmsg("argument to option\"%s\" must be a valid encoding name", optionDef->defname)));
    } else {
        VerifyEncoding(encoding);
    }
}

static inline bool IsOrcSupportType(Oid TypeOid)
{
    return (TypeOid == BOOLOID || TypeOid == BPCHAROID || TypeOid == DATEOID || TypeOid == FLOAT4OID ||
            TypeOid == FLOAT8OID || TypeOid == INT1OID || TypeOid == INT2OID || TypeOid == INT4OID ||
            TypeOid == INT8OID || TypeOid == NUMERICOID || TypeOid == TEXTOID || TypeOid == TIMESTAMPOID ||
            TypeOid == VARCHAROID || TypeOid == CLOBOID);
}

void DFSCheckDataType(TypeName *typName, char *ColName, int format)
{
    Oid TypeOid = typenameTypeId(NULL, typName);

    bool IsValidOrcType = IsOrcSupportType(TypeOid);
    bool IsValidCarbondataType = (IsValidOrcType || TypeOid == BYTEAOID);

    if (format == DFS_CARBONDATA && !IsValidCarbondataType) {
        ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE), errmodule(MOD_DFS),
                        errmsg("Column %s is unsupported data type.", ColName)));
    } else if (format != DFS_CARBONDATA && !IsValidOrcType) {
        ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE), errmodule(MOD_DFS),
                        errmsg("Column %s is unsupported data type.", ColName)));
    }

    if (TypeOid == DATEOID && u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_DATA_TYPE), errmsg("Date type is only support in A-format database.")));
    }

    /*
     * Check decimal/numeric accuracy.
     */
    if (NUMERICOID == TypeOid) {
        CheckNumericAccuracy(typName);
    }
}
