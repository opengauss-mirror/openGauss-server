#ifndef COMMON_ERR_H
#define COMMON_ERR_H


#ifdef __cplusplus
extern "C" {
#endif

/*硬件内部错误码*/
#define INTERNAL_OK 0

#define INTERNAL_BASE_ERR    0x01000000

#define INTERNAL_UNKNOWNERR              (INTERNAL_BASE_ERR + 0x00000001) /* 未知错误 */
#define INTERNAL_NOTSUPPORT              (INTERNAL_BASE_ERR + 0x00000002) /* 不支持 */
#define INTERNAL_COMMFAIL                (INTERNAL_BASE_ERR + 0x00000003) /* 通信错误 */
#define INTERNAL_HARDFAIL                (INTERNAL_BASE_ERR + 0x00000004) /* 硬件错误 */
#define INTERNAL_OPENDEVICE              (INTERNAL_BASE_ERR + 0x00000005) /* 打开设备错误 */
#define INTERNAL_OPENSESSION             (INTERNAL_BASE_ERR + 0x00000006) /* 打开会话句柄错误 */
#define INTERNAL_PARDENY                 (INTERNAL_BASE_ERR + 0x00000007) /* 权限不满足 */
#define INTERNAL_KEYNOTEXIST             (INTERNAL_BASE_ERR + 0x00000008) /* 密钥不存在 */
#define INTERNAL_ALGNOTSUPPORT           (INTERNAL_BASE_ERR + 0x00000009) /* 不支持的算法 */
#define INTERNAL_ALGMODNOTSUPPORT        (INTERNAL_BASE_ERR + 0x0000000A) /* 不支持的算法模式 */
#define INTERNAL_PKOPERR                 (INTERNAL_BASE_ERR + 0x0000000B) /* 公钥运算错误 */
#define INTERNAL_SKOPERR                 (INTERNAL_BASE_ERR + 0x0000000C) /* 私钥运算错误 */
#define INTERNAL_SIGNERR                 (INTERNAL_BASE_ERR + 0x0000000D) /* 签名错误 */
#define INTERNAL_VERIFYERR               (INTERNAL_BASE_ERR + 0x0000000E) /* 验证错误 */
#define INTERNAL_SYMOPERR                (INTERNAL_BASE_ERR + 0x0000000F) /* 对称运算错误 */
#define INTERNAL_STEPERR                 (INTERNAL_BASE_ERR + 0x00000010) /* 步骤错误 */
#define INTERNAL_FILESIZEERR             (INTERNAL_BASE_ERR + 0x00000011) /* 文件大小错误或输入数据长度非法 */
#define INTERNAL_FILENOEXIST             (INTERNAL_BASE_ERR + 0x00000012) /* 文件不存在 */
#define INTERNAL_FILEOFSERR              (INTERNAL_BASE_ERR + 0x00000013) /* 文件操作偏移量错误 */
#define INTERNAL_KEYTYPEERR              (INTERNAL_BASE_ERR + 0x00000014) /* 密钥类型错误 */
#define INTERNAL_KEYERR                  (INTERNAL_BASE_ERR + 0x00000015) /* 密钥错误 */
#define INTERNAL_ENCDATAERR              (INTERNAL_BASE_ERR + 0x00000016) /* 加密数据错误 */
#define INTERNAL_RANDERR                 (INTERNAL_BASE_ERR + 0x00000017) /* 随机数产生失败 */
#define INTERNAL_PRKRERR                 (INTERNAL_BASE_ERR + 0x00000018) /* 私钥使用权限获取失败 */
#define INTERNAL_MACERR                  (INTERNAL_BASE_ERR + 0x00000019) /* MAC 运算失败 */
#define INTERNAL_FILEEXISTS              (INTERNAL_BASE_ERR + 0x0000001A) /* 指定文件已存在 */
#define INTERNAL_FILEWERR                (INTERNAL_BASE_ERR + 0x0000001B) /* 文件写入失败 */
#define INTERNAL_NOBUFFER                (INTERNAL_BASE_ERR + 0x0000001C) /* 存储空间不足 */
#define INTERNAL_INARGERR                (INTERNAL_BASE_ERR + 0x0000001D) /* 输入参数错误 */
#define INTERNAL_OUTARGERR               (INTERNAL_BASE_ERR + 0x0000001E) /* 输出参数错误 */
#define INTERNAL_UKEYERR                 (INTERNAL_BASE_ERR + 0x0000001F) /* Ukey错误 */
#define INTERNAL_GENKEYERR               (INTERNAL_BASE_ERR + 0x00000020) /* 密钥生成错误 */
#define INTERNAL_STATEERR                (INTERNAL_BASE_ERR + 0x00000021) /* 状态错误 */
#define INTERNAL_RETRYERR                (INTERNAL_BASE_ERR + 0x00000022) /* 重试超过次数 */
#define INTERNAL_DEVICE_BUSY             (INTERNAL_BASE_ERR + 0x00000023) /* 设备忙 */


/*库中自定义错误码*/
/*特别注意，硬件密码模块的返回值是0表示成功，非0表示失败（错误码），和库对外返回的不一样*/
#define CRYPT_MOD_OK 1
#define CRYPT_MOD_ERR 0

#define CRYPTO_MOD_BASE_ERR 0x01000

#define CRYPTO_MOD_TYPE_REPEATED_ERR                (CRYPTO_MOD_BASE_ERR + 0x00001)/*密码模块类型重复设置*/
#define CRYPTO_MOD_TYPE_INVALID_ERR                 (CRYPTO_MOD_BASE_ERR + 0x00002)/*无效的密码模块类型*/
#define CRYPTO_MOD_LIBPATH_REPEATED_ERR             (CRYPTO_MOD_BASE_ERR + 0x00003)/*密码模块库路径重复设置*/
#define CRYPTO_MOD_LIBPATH_INVALID_ERR              (CRYPTO_MOD_BASE_ERR + 0x00004)/*无效的密码模块库路径*/
#define CRYPTO_MOD_CFG_PATH_REPEATED_ERR            (CRYPTO_MOD_BASE_ERR + 0x00005)/*密码模块配置文件重复设置*/
#define CRYPTO_MOD_CFG_PATH_INVALID_ERR             (CRYPTO_MOD_BASE_ERR + 0x00006)/*无效的密码模块配置文件*/
#define CRYPTO_MOD_PARAM_TOO_MANY_ERR               (CRYPTO_MOD_BASE_ERR + 0x00007)/*密码卡参数配置过多*/
#define CRYPTO_MOD_PARAM_INVALID_ERR                (CRYPTO_MOD_BASE_ERR + 0x00008)/*无效的参数*/
#define CRYPTO_MOD_UNSUPPORTED_SYMM_TYPE_ERR        (CRYPTO_MOD_BASE_ERR + 0x00009)/*不支持的对称算法类型*/
#define CRYPTO_MOD_UNSUPPORTED_DIGEST_TYPE_ERR      (CRYPTO_MOD_BASE_ERR + 0x0000A)/*不支持的摘要算法类型*/
#define CRYPTO_MOD_DLOPEN_ERR                       (CRYPTO_MOD_BASE_ERR + 0x0000B)/*dlopen失败*/
#define CRYPTO_MOD_DLSYM_ERR                        (CRYPTO_MOD_BASE_ERR + 0x0000C)/*dlsym失败*/
#define CRYPTO_MOD_UNLOAD_ERR                       (CRYPTO_MOD_BASE_ERR + 0x0000D)/*dlclose失败*/
#define CRYPTO_MOD_NOT_LOADED_ERR                   (CRYPTO_MOD_BASE_ERR + 0x0000E)/*还未加载驱动库*/
#define CRYPTO_MOD_NOT_OPENDEVICE_ERR               (CRYPTO_MOD_BASE_ERR + 0x0000F)/*还未打开设备*/
#define CRYPTO_MOD_NOT_OPENSESSION_ERR              (CRYPTO_MOD_BASE_ERR + 0x00010)/*还未建立会话*/
#define CRYPTO_MOD_INVALID_KEY_ERR                  (CRYPTO_MOD_BASE_ERR + 0x00011)/*无效的密钥*/
#define CRYPTO_MOD_INVALID_CRYPTO_TYPE_ERR          (CRYPTO_MOD_BASE_ERR + 0x00012)/*无效的加解密类型*/
#define CRYPTO_MOD_INVALID_KEY_CTX_ERR              (CRYPTO_MOD_BASE_ERR + 0x00013)/*无效密钥上下文*/
#define CRYPTO_MOD_UNPADDING_ERR                    (CRYPTO_MOD_BASE_ERR + 0x00014)/*去pad失败*/
#define CRYPTO_MOD_NOT_ENOUGH_SPACE_ERR             (CRYPTO_MOD_BASE_ERR + 0x00015)/*分配的空间不足*/
#define CRYPTO_MOD_DETERMINISTIC_DEC_VERIFY_ERR     (CRYPTO_MOD_BASE_ERR + 0x00016)/*确定性解密校验失败*/
#define CRYPTO_MOD_UNKNOWN_PARAM_ERR                (CRYPTO_MOD_BASE_ERR + 0x00017)/*未知的参数*/

extern void set_thread_errno(int errno);
extern const char* common_get_errmsg();

#ifdef __cplusplus
}
#endif

#endif /* COMMON_ERR_H */
