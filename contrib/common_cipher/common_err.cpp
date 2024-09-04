#include "common_err.h"

#define MAX_ERRMSG_LEN 256

#define lengthof(array) (sizeof(array) / sizeof(array[0]))

static thread_local int internalthreaderrno = 0;

static void reset_thread_errno();

typedef struct {
    int errno;
    char errmsg[MAX_ERRMSG_LEN];
}CommonErrInfo;

/*HSM:hardware security module,硬件安全模块。*/
static const CommonErrInfo internal_err_info[] = {
    {INTERNAL_UNKNOWNERR, "HSM unknown error"},
    {INTERNAL_NOTSUPPORT, "HSM not support"},
    {INTERNAL_COMMFAIL, "HSM communication error"},
    {INTERNAL_HARDFAIL, "HSM hard error"},
    {INTERNAL_OPENDEVICE, "HSM open device error"},
    {INTERNAL_OPENSESSION, "HSM open session error"},
    {INTERNAL_PARDENY, "HSM permission error"},
    {INTERNAL_KEYNOTEXIST, "HSM key not exist"},
    {INTERNAL_ALGNOTSUPPORT, "HSM not support algorithm"},
    {INTERNAL_ALGMODNOTSUPPORT, "HSM not support algorithm mode"},
    {INTERNAL_PKOPERR, "HSM public key operation error"},
    {INTERNAL_SKOPERR, "HSM private key operation error"},
    {INTERNAL_SIGNERR, "HSM sign error"},
    {INTERNAL_VERIFYERR, "HSM verify error"},
    {INTERNAL_SYMOPERR, "HSM symmetry operation error"},
    {INTERNAL_STEPERR, "HSM step error"},
    {INTERNAL_FILESIZEERR, "HSM file size or data len error"},
    {INTERNAL_FILENOEXIST, "HSM file not exist"},
    {INTERNAL_FILEOFSERR, "HSM file offset operation error"},
    {INTERNAL_KEYTYPEERR, "HSM key type error"},
    {INTERNAL_KEYERR, "HSM key error"},
    {INTERNAL_ENCDATAERR, "HSM encrypt data error"},
    {INTERNAL_RANDERR, "HSM random error"},
    {INTERNAL_PRKRERR, "HSM private access right error"},
    {INTERNAL_MACERR, "HSM MAC error"},
    {INTERNAL_FILEEXISTS, "HSM file exists"},
    {INTERNAL_FILEWERR, "HSM write file error"},
    {INTERNAL_NOBUFFER, "HSM not enough storage"},
    {INTERNAL_INARGERR, "HSM input param error"},
    {INTERNAL_OUTARGERR, "HSM output param error"},
    {INTERNAL_UKEYERR, "HSM ukey error"},
    {INTERNAL_GENKEYERR, "HSM generate key error"},
    {INTERNAL_STATEERR, "HSM status error"},
    {INTERNAL_RETRYERR, "HSM retry exceeded"},
    {INTERNAL_DEVICE_BUSY, "HSM is busy"}
};

static int internal_err_number = lengthof(internal_err_info);

static const CommonErrInfo common_err_info[] = {
    {CRYPTO_MOD_TYPE_REPEATED_ERR, "module type set repeated"},
    {CRYPTO_MOD_TYPE_INVALID_ERR, "invalid module type"},
    {CRYPTO_MOD_LIBPATH_REPEATED_ERR, "module lib path set repeated"},
    {CRYPTO_MOD_LIBPATH_INVALID_ERR, "invalid module lib path"},
    {CRYPTO_MOD_CFG_PATH_REPEATED_ERR, "module config file set repeated"},
    {CRYPTO_MOD_CFG_PATH_INVALID_ERR, "invalid module config file"},
    {CRYPTO_MOD_PARAM_TOO_MANY_ERR, "param is too many"},
    {CRYPTO_MOD_PARAM_INVALID_ERR, "invalid param"},
    {CRYPTO_MOD_UNSUPPORTED_SYMM_TYPE_ERR, "unsupported symm algo type"},
    {CRYPTO_MOD_UNSUPPORTED_DIGEST_TYPE_ERR, "unsupported digest algo type"},
    {CRYPTO_MOD_DLOPEN_ERR, "dlopen error"},
    {CRYPTO_MOD_DLSYM_ERR, "dlsym error"},
    {CRYPTO_MOD_UNLOAD_ERR, "unload error"},
    {CRYPTO_MOD_NOT_LOADED_ERR, "module not loaded"},
    {CRYPTO_MOD_NOT_OPENDEVICE_ERR, "device not opened"},
    {CRYPTO_MOD_NOT_OPENSESSION_ERR, "session not opened"},
    {CRYPTO_MOD_INVALID_KEY_ERR, "invalid key"},
    {CRYPTO_MOD_INVALID_CRYPTO_TYPE_ERR, "invalid crypto type"},
    {CRYPTO_MOD_INVALID_KEY_CTX_ERR, "invalid key ctx"},
    {CRYPTO_MOD_UNPADDING_ERR, "unpadding err"},
    {CRYPTO_MOD_NOT_ENOUGH_SPACE_ERR, "not enough space"},
    {CRYPTO_MOD_DETERMINISTIC_DEC_VERIFY_ERR, "deterministic dec verify error"},
    {CRYPTO_MOD_UNKNOWN_PARAM_ERR, "unknown module param"}
};

static int common_err_number = lengthof(common_err_info);

static const char* unknown_err = "unknown err";

const char* common_get_errmsg()
{
    int i = 0;

    if (internalthreaderrno & INTERNAL_BASE_ERR) {
        for (i = 0; i < internal_err_number; i++) {
            if (internalthreaderrno == internal_err_info[i].errno) {
                reset_thread_errno();
                return internal_err_info[i].errmsg;
            }
        }
    } else if (internalthreaderrno & CRYPTO_MOD_BASE_ERR) {
        for (i = 0; i < common_err_number; i++) {
            if (internalthreaderrno == common_err_info[i].errno) {
                reset_thread_errno();
                return common_err_info[i].errmsg;
            }
        }
    } else {
        reset_thread_errno();
        return unknown_err;
    }
    
    if (i != internal_err_number && i != common_err_number) {
        reset_thread_errno();
        return unknown_err;
    }

    return unknown_err;
}

void set_thread_errno(int errno)
{
    internalthreaderrno = errno;
}

static void reset_thread_errno()
{
    internalthreaderrno = 0;
}
