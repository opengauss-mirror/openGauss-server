#ifndef COMMON_UTILS_H
#define COMMON_UTILS_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    MODULE_INVALID_TYPE = 0,
    MODULE_GDAC_CARD_TYPE,  /*光电安辰密码卡*/
    MODULE_JNTA_KMS_TYPE,   /*江南天安KMS*/
    MODULE_SWXA_KMS_TYPE    /*三未信安KMS*/
} ModuleType;

/*定义以下字符串宏，用来对输入的kv结构字符串做解析，获取对应的value*/
#define MODULE_TYPE "MODULE_TYPE"
#define MODULE_LIB_PATH "MODULE_LIB_PATH"
#define MODULE_CONFIG_FILE_PATH "MODULE_CONFIG_FILE_PATH"

/*支持的硬件类型字符串*/
#define MODULE_GDAC_CARD_STR "GDACCARD"
#define MODULE_JNTA_KMS_STR "JNTAKMS"
#define MODULE_SWXA_KMS_STR "SWXAKMS"

#define IS_GDAC_CARD_TYPE(s) (strcmp(s, MODULE_GDAC_CARD_STR) == 0)
#define IS_JNTA_KMS_TYPE(s) (strcmp(s, MODULE_JNTA_KMS_STR) == 0)
#define IS_SWXA_KMS_TYPE(s) (strcmp(s, MODULE_SWXA_KMS_STR) == 0)

/*字符串转为枚举类型*/
#define GET_MODULE_TYPE(s) (IS_GDAC_CARD_TYPE(s) ? MODULE_GDAC_CARD_TYPE \
    : IS_JNTA_KMS_TYPE(s) ? MODULE_JNTA_KMS_TYPE \
    : IS_SWXA_KMS_TYPE(s) ? MODULE_SWXA_KMS_TYPE : MODULE_INVALID_TYPE)

#define IS_MODULE_TYPE(s) (strcmp(s, MODULE_TYPE) == 0)
#define IS_MODULE_LIB_PATH(s) (strcmp(s, MODULE_LIB_PATH) == 0)
#define IS_MODULE_CONFIG_FILE_PATH(s) (strcmp(s, MODULE_CONFIG_FILE_PATH) == 0)

#define MODULE_MAX_PATH_LEN 1024

typedef struct {
    ModuleType moduletype;
    char libpath[MODULE_MAX_PATH_LEN];
    char cfgfilepath[MODULE_MAX_PATH_LEN];
}ModuleParams;

extern int parse_module_params(char *paramsstring, ModuleParams *moduleparams);

#ifdef __cplusplus
}
#endif

#endif /* COMMON_UTILS_H */
