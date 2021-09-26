#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#if defined __GNUC__ || defined LINUX
#include <string.h>
#include <getopt.h>
#include <strings.h>
#include <pthread.h>
#include <unistd.h>
#else
#include "getopt.h"
#endif

#include "eSDKOBS.h"
#include "securec.h"

/* Some Windows stuff */
#ifndef FOPEN_EXTRA_FLAGS
#define FOPEN_EXTRA_FLAGS ""
#endif

/* Some Unix stuff (to work around Windows issues) */
#ifndef SLEEP_UNITS_PER_SECOND
#define SLEEP_UNITS_PER_SECOND 1
#endif

#ifndef SLEEP_UNITS_PER_SECOND_WIN
#define SLEEP_UNITS_PER_SECOND_WIN 1000
#endif

#ifdef _MSC_VER
#define snprintf_s _snprintf_s
#endif

/* Also needed for Windows, because somehow MinGW doesn't define this */
#if defined __GNUC__ || defined LINUX
extern int putenv(char*);
#endif

#if defined __GNUC__ || defined LINUX
/* _TRUNCATE */
#define _TRUNCATE ((size_t)-1)
#endif

/* Command-line options, saved as globals */
static int forceG = 0;
static int showResponsePropertiesG = 0;
static obs_protocol protocolG = OBS_PROTOCOL_HTTP;
static obs_uri_style uriStyleG = OBS_URI_STYLE_PATH;
static int retriesG = 5;

/* Windows globals */
#if defined WIN32
#define BAD_OPTION '\0'
#define no_argument 0
#define required_argument 1
#define optional_argument 2

static int first_nonopt;
static int last_nonopt;
char* optarg = 0;
int optind = 0;
int optopt = BAD_OPTION;
static char* nextchar;
static enum { REQUIRE_ORDER, PERMUTE, RETURN_IN_ORDER } ordering;
int opterr = 1;

struct option {
    char* name;
    int has_arg;
    int* flag;
    int val;
};
#endif

/* Environment variables, saved as globals */
static char* accessKeyIdG = 0;
static char* secretAccessKeyG = 0;
static char* hostName = 0;

/* Request results, saved as globals */
static obs_status statusG = OBS_STATUS_OK;
static char errorDetailsG[4096] = {0};

/* Other globals */
static char putenvBufG[256] = {0};

/* Certificate Information */
static char* pCAInfo = 0;

/* obs init function */
static void OBS_init()
{
    obs_status status = OBS_STATUS_BUTT;

    if ((status = obs_initialize(OBS_INIT_ALL)) != OBS_STATUS_OK) {
        fprintf(stderr, "Failed to initialize libobs: %s\n", obs_get_status_name(status));
        exit(-1);
    }
}

void printError()
{
    if (statusG < OBS_STATUS_AccessDenied) {
        fprintf(stderr, "\nERROR: %s\n", obs_get_status_name(statusG));
    } else {
        fprintf(stderr, "\nERROR: %s\n", obs_get_status_name(statusG));
        fprintf(stderr, "%s\n", errorDetailsG);
    }
}

static int should_retry()
{
    if (retriesG-- > 0) {
        /* Sleep before next retry; start out with a 1 second sleep */
        static int retrySleepInterval = 1;
        (void)sleep(retrySleepInterval);
        /* Next sleep 1 second longer */
        retrySleepInterval++;
        return 1;
    }

    return 0;
}

static void usageExit(FILE* out)
{
    fprintf(out,
        "\n Options:\n"
        "\n"
        "   Command Line:\n"
        "\n"
        "   -f/--force           : force operation despite warnings\n"
        "   -h/--vhost-style     : use virtual-host-style URIs (default is "
        "path-style)\n"
        "   -u/--unencrypted     : unencrypted (use HTTP instead of HTTPS)\n"
        "   -s/--show-properties : show response properties on stdout\n"
        "   -r/--retries         : retry retryable failures this number of times\n"
        "                          (default is 5)\n"
        "\n"
        "   Environment:\n"
        "\n"
        "   OBS_ACCESS_KEY_ID     : OBS access key ID (required)\n"
        "   OBS_SECRET_ACCESS_KEY : OBS secret access key (required)\n"
        "   OBS_HOSTNAME          : specify alternative S3 host (optional)\n"
        "\n"
        " Commands (with <required parameters> and [optional parameters]) :\n"
        "\n"
        "   (NOTE: all command parameters take a value and are specified using the\n"
        "          pattern parameter=value)\n"
        "\n"
        "   help                 : Prints this help text\n"
        "\n"
        "   delete               : Delete a bucket or key\n"
        "     <bucket>[/<key>]   : Bucket or bucket/key to delete\n");

    exit(-1);
}

static uint64_t convertInt(const char* str, const char* paramName)
{
    uint64_t ret = 0;

    while (*str) {
        if (!isdigit(*str)) {
            fprintf(stderr, "\nERROR: Nondigit in %s parameter: %c\n", paramName, *str);
            usageExit(stderr);
        }
        ret *= 10;
        ret += (*str++ - '0');
    }

    return ret;
}

// Convenience utility for making the code look nicer.  Tests a string
// against a format; only the characters specified in the format are
// checked (i.e. if the string is longer than the format, the string still
// checks out ok).  Format characters are:
// d - is a digit
// anything else - is that character
// Returns nonzero the string checks out, zero if it does not.
static int checkString(const char* str, const char* format)
{
    while (*format) {
        if (*format == 'd') {
            if (!isdigit(*str)) {
                return 0;
            }
        } else if (*str != *format) {
            return 0;
        }
        str++, format++;
    }

    return 1;
}

static int64_t parseIso8601Time(const char* str)
{
    // Check to make sure that it has a valid format
    if (!checkString(str, "dddd-dd-ddTdd:dd:dd")) {
        return -1;
    }

#define nextnum() (((*str - '0') * 10) + (*(str + 1) - '0'))

    // Convert it
    struct tm stm;
    memset_s(&stm, sizeof(stm), 0, sizeof(stm));

    stm.tm_year = (nextnum() - 19) * 100;
    str += 2;
    stm.tm_year += nextnum();
    str += 3;

    stm.tm_mon = nextnum() - 1;
    str += 3;

    stm.tm_mday = nextnum();
    str += 3;

    stm.tm_hour = nextnum();
    str += 3;

    stm.tm_min = nextnum();
    str += 3;

    stm.tm_sec = nextnum();
    str += 2;

    stm.tm_isdst = -1;

    // This is hokey but it's the recommended way ...
    char* tz = getenv("TZ");
    snprintf_s(putenvBufG, sizeof(putenvBufG), _TRUNCATE, "TZ=UTC");
    putenv(putenvBufG);

    int64_t ret = mktime(&stm);

    snprintf_s(putenvBufG, sizeof(putenvBufG), _TRUNCATE, "TZ=%s", tz ? tz : "");
    putenv(putenvBufG);

    // Skip the millis

    if (*str == '.') {
        str++;
        while (isdigit(*str)) {
            str++;
        }
    }

    if (checkString(str, "-dd:dd") || checkString(str, "+dd:dd")) {
        int sign = (*str++ == '-') ? -1 : 1;
        int hours = nextnum();
        str += 3;
        int minutes = nextnum();
        ret += (-sign * (((hours * 60) + minutes) * 60));
    }
    // Else it should be Z to be a conformant time string, but we just assume
    // that it is rather than enforcing that

    return ret;
}

//lint -e121
static struct option longOptionsG[] = {{"force", no_argument, 0, 'f'},  //lint !e155
    {"vhost-style", no_argument, 0, 'h'},
    {"unencrypted", no_argument, 0, 'u'},
    {"show-properties", no_argument, 0, 's'},
    {"retries", required_argument, 0, 'r'},
    {0, 0, 0, 0}};
//lint +e121

/*
 * response properties callback
 * This callback does the same thing for every request type: prints out the
 * properties if the user has requested them to be so
 */
static obs_status responsePropertiesCallback(const obs_response_properties* properties, void* callbackData)
{
    return OBS_STATUS_OK;
}

/*
 * response complete callback
 * This callback does the same thing for every request type: saves the status
 * and error stuff in global variables
 */
static void responseCompleteCallback(obs_status status, const obs_error_details* error, void* callbackData)
{
    (void)callbackData;

    statusG = status;
    /*
     * Compose the error details message now, although we might not use it.
     * Can't just save a pointer to [error] since it's not guaranteed to last
     * beyond this callback
     */
    int len = 0;
    if (error && error->message) {
        len += snprintf_s(
            &(errorDetailsG[len]), sizeof(errorDetailsG) - len, _TRUNCATE, "  Message: %s\n", error->message);
    }
    if (error && error->resource) {
        len += snprintf_s(
            &(errorDetailsG[len]), sizeof(errorDetailsG) - len, _TRUNCATE, "  Resource: %s\n", error->resource);
    }
    if (error && error->further_details) {
        len += snprintf_s(&(errorDetailsG[len]),
            sizeof(errorDetailsG) - len,
            _TRUNCATE,
            "  Further Details: %s\n",
            error->further_details);
    }
    if (error && error->extra_details_count) {
        len += snprintf_s(&(errorDetailsG[len]), sizeof(errorDetailsG) - len, _TRUNCATE, "%s", "  Extra Details:\n");
        int i;
        for (i = 0; i < error->extra_details_count; i++) {
            len += snprintf_s(&(errorDetailsG[len]),
                sizeof(errorDetailsG) - len,
                _TRUNCATE,
                "    %s: %s\n",
                error->extra_details[i].name,
                error->extra_details[i].value);
        }
    }
}

typedef struct ListObjectsCallBackData {
    int objectsNum;
    char bucket[1000][1000];
} ListObjectsCallBackData;

static obs_status listBucketObjectsCallback(int isTruncated, const char* nextMarker, int contentsCount,
    const obs_list_objects_content* contents, int commonPrefixesCount, const char** commonPrefixes, void* callbackData)
{
    /* change vector to List */
    ListObjectsCallBackData* objects = (ListObjectsCallBackData*)callbackData;

    /* Defence: libobs make callbackData to NULL in some condition */
    int i = 0;
    objects->objectsNum = contentsCount;
    for (i = 0; i < contentsCount; i++) {
        const obs_list_objects_content* content = &(contents[i]);
        strcpy(objects->bucket[i], content->key);
    }

    return OBS_STATUS_OK;
}

static void list_obs_objects(char* bucketName, char* key, ListObjectsCallBackData* objectsData)
{
    OBS_init();

    /* initialize options */
    obs_options option;
    init_obs_options(&option);

    /* fill the bucket context content */
    option.bucket_options.host_name = hostName;
    option.bucket_options.bucket_name = bucketName;
    option.bucket_options.protocol = OBS_PROTOCOL_HTTP;
    option.bucket_options.uri_style = uriStyleG;
    option.bucket_options.access_key = accessKeyIdG;
    option.bucket_options.secret_access_key = secretAccessKeyG;
    option.bucket_options.certificate_info = NULL;

    obs_list_objects_handler listBucketHandler = {
        {&responsePropertiesCallback, &responseCompleteCallback}, &listBucketObjectsCallback};

    do {
        list_bucket_objects(&option,
            key,
            NULL /* marker */,
            NULL /* delimiter */,
            0 /* maxkeys */,
            &listBucketHandler,
            (void*)objectsData);
    } while (obs_status_is_retryable(statusG) && should_retry(retriesG));

    obs_deinitialize();
}

/* delete objects */
static void delete_obs_objects(char* bucketName, char* key)
{
    OBS_init();

    obs_options option;
    init_obs_options(&option);

    /* fill the bucket context content */
    option.bucket_options.host_name = hostName;
    option.bucket_options.bucket_name = bucketName;
    option.bucket_options.protocol = protocolG;
    option.bucket_options.uri_style = uriStyleG;
    option.bucket_options.access_key = accessKeyIdG;
    option.bucket_options.secret_access_key = secretAccessKeyG;
    option.bucket_options.certificate_info = pCAInfo;

    /* init object_info */
    char* versionId = NULL;
    obs_object_info object_info;
    memset(&object_info, 0, sizeof(obs_object_info));
    object_info.key = key;
    object_info.version_id = versionId;

    obs_response_handler responseHandler = {responsePropertiesCallback, &responseCompleteCallback};

    do {
        delete_object(&option, &object_info, &responseHandler, 0);
    } while (obs_status_is_retryable(statusG) && should_retry());

    if ((statusG != OBS_STATUS_OK) && (statusG != OBS_STATUS_PreconditionFailed)) {
        printError();
    }

    obs_deinitialize();
}

#if defined WIN32
static char* my_index(const char* str, int chr)
{
    while (*str) {
        if (*str == chr)
            return (char*)str;
        str++;
    }
    return 0;
}

static void exchange(char** argv)
{
    char *temp, **first, **last;

    /* Reverse all the elements [first_nonopt, optind) */
    first = &argv[first_nonopt];
    last = &argv[optind - 1];
    while (first < last) {
        temp = *first;
        *first = *last;
        *last = temp;
        first++;
        last--;
    }
    /* Put back the options in order */
    first = &argv[first_nonopt];
    first_nonopt += (optind - last_nonopt);
    last = &argv[first_nonopt - 1];
    while (first < last) {
        temp = *first;
        *first = *last;
        *last = temp;
        first++;
        last--;
    }

    /* Put back the non options in order */
    first = &argv[first_nonopt];
    last_nonopt = optind;
    last = &argv[last_nonopt - 1];
    while (first < last) {
        temp = *first;
        *first = *last;
        *last = temp;
        first++;
        last--;
    }
}

int _getopt_internal(
    int argc, char* const* argv, const char* optstring, const struct option* longopts, int* longind, int long_only)
{
    int option_index;

    optarg = 0;

    /* Initialize the internal data when the first call is made.
       Start processing options with ARGV-element 1 (since ARGV-element 0
       is the program name); the sequence of previously skipped
       non-option ARGV-elements is empty.  */

    if (optind == 0) {
        first_nonopt = last_nonopt = optind = 1;

        nextchar = NULL;

        /* Determine how to handle the ordering of options and nonoptions.  */

        if (optstring[0] == '-') {
            ordering = RETURN_IN_ORDER;
            ++optstring;
        } else if (optstring[0] == '+') {
            ordering = REQUIRE_ORDER;
            ++optstring;
        } else if (getenv("POSIXLY_CORRECT") != NULL)
            ordering = REQUIRE_ORDER;
        else
            ordering = PERMUTE;
    }

    if (nextchar == NULL || *nextchar == '\0') {
        if (ordering == PERMUTE) {
            /* If we have just processed some options following some non-options,
               exchange them so that the options come first.  */

            if (first_nonopt != last_nonopt && last_nonopt != optind)
                exchange((char**)argv);
            else if (last_nonopt != optind)
                first_nonopt = optind;

            /* Now skip any additional non-options
               and extend the range of non-options previously skipped.  */

            while (optind < argc && (argv[optind][0] != '-' || argv[optind][1] == '\0'))
                optind++;
            last_nonopt = optind;
        }

        /* Special ARGV-element `--' means premature end of options.
       Skip it like a null option,
       then exchange with previous non-options as if it were an option,
       then skip everything else like a non-option.  */

        if (optind != argc && !strcmp(argv[optind], "--")) {
            optind++;

            if (first_nonopt != last_nonopt && last_nonopt != optind)
                exchange((char**)argv);
            else if (first_nonopt == last_nonopt)
                first_nonopt = optind;
            last_nonopt = argc;

            optind = argc;
        }

        /* If we have done all the ARGV-elements, stop the scan
       and back over any non-options that we skipped and permuted.  */

        if (optind == argc) {
            /* Set the next-arg-index to point at the non-options
               that we previously skipped, so the caller will digest them.  */
            if (first_nonopt != last_nonopt)
                optind = first_nonopt;
            return EOF;
        }

        /* If we have come to a non-option and did not permute it,
       either stop the scan or describe it to the caller and pass it by.  */

        if ((argv[optind][0] != '-' || argv[optind][1] == '\0')) {
            if (ordering == REQUIRE_ORDER)
                return EOF;
            optarg = argv[optind++];
            return 1;
        }

        /* We have found another option-ARGV-element.
       Start decoding its characters.  */

        nextchar = (argv[optind] + 1 + (longopts != NULL && argv[optind][1] == '-'));
    }

    if (longopts != NULL && ((argv[optind][0] == '-' && (argv[optind][1] == '-' || long_only)))) {
        const struct option* p;
        char* s = nextchar;
        int exact = 0;
        int ambig = 0;
        const struct option* pfound = NULL;
        int indfound = 0;

        while (*s && *s != '=')
            s++;

        /* Test all options for either exact match or abbreviated matches.  */
        for (p = longopts, option_index = 0; p->name; p++, option_index++)
            if (!strncmp(p->name, nextchar, s - nextchar)) {
                if (s - nextchar == strlen(p->name)) {
                    /* Exact match found.  */
                    pfound = p;
                    indfound = option_index;
                    exact = 1;
                    break;
                } else if (pfound == NULL) {
                    /* First nonexact match found.  */
                    pfound = p;
                    indfound = option_index;
                } else
                    /* Second nonexact match found.  */
                    ambig = 1;
            }

        if (ambig && !exact) {
            if (opterr)
                fprintf(stderr, "%s: option `%s' is ambiguous\n", argv[0], argv[optind]);
            nextchar += strlen(nextchar);
            optind++;
            return BAD_OPTION;
        }

        if (pfound != NULL) {
            option_index = indfound;
            optind++;
            if (*s) {
                /* Don't test has_arg with >, because some C compilers don't
               allow it to be used on enums.  */
                if (pfound->has_arg)
                    optarg = s + 1;
                else {
                    if (opterr) {
                        if (argv[optind - 1][1] == '-')
                            /* --option */
                            fprintf(stderr, "%s: option `--%s' doesn't allow an argument\n", argv[0], pfound->name);
                        else
                            /* +option or -option */
                            fprintf(stderr,
                                "%s: option `%c%s' doesn't allow an argument\n",
                                argv[0],
                                argv[optind - 1][0],
                                pfound->name);
                    }
                    nextchar += strlen(nextchar);
                    return BAD_OPTION;
                }
            } else if (pfound->has_arg == 1) {
                if (optind < argc)
                    optarg = argv[optind++];
                else {
                    if (opterr)
                        fprintf(stderr, "%s: option `%s' requires an argument\n", argv[0], argv[optind - 1]);
                    nextchar += strlen(nextchar);
                    return optstring[0] == ':' ? ':' : BAD_OPTION;
                }
            }
            nextchar += strlen(nextchar);
            if (longind != NULL)
                *longind = option_index;
            if (pfound->flag) {
                *(pfound->flag) = pfound->val;
                return 0;
            }
            return pfound->val;
        }
        /* Can't find it as a long option.  If this is not getopt_long_only,
       or the option starts with '--' or is not a valid short
       option, then it's an error.
       Otherwise interpret it as a short option.  */
        if (!long_only || argv[optind][1] == '-' || my_index(optstring, *nextchar) == NULL) {
            if (opterr) {
                if (argv[optind][1] == '-')
                    /* --option */
                    fprintf(stderr, "%s: unrecognized option `--%s'\n", argv[0], nextchar);
                else
                    /* +option or -option */
                    fprintf(stderr, "%s: unrecognized option `%c%s'\n", argv[0], argv[optind][0], nextchar);
            }
            nextchar = (char*)"";
            optind++;
            return BAD_OPTION;
        }
    }

    /* Look at and handle the next option-character.  */

    {
        char c = *nextchar++;
        char* temp = my_index(optstring, c);

        /* Increment `optind' when we start to process its last character.  */
        if (*nextchar == '\0')
            ++optind;

        if (temp == NULL || c == ':') {
            if (opterr) {
                /* 1003.2 specifies the format of this message.  */
                fprintf(stderr, "%s: illegal option -- %c\n", argv[0], c);
            }
            optopt = c;
            return BAD_OPTION;
        }
        if (temp[1] == ':') {
            if (temp[2] == ':') {
                /* This is an option that accepts an argument optionally.  */
                if (*nextchar != '\0') {
                    optarg = nextchar;
                    optind++;
                } else
                    optarg = 0;
                nextchar = NULL;
            } else {
                /* This is an option that requires an argument.  */
                if (*nextchar != '\0') {
                    optarg = nextchar;
                    /* If we end this ARGV-element by taking the rest as an arg,
                       we must advance to the next element now.  */
                    optind++;
                } else if (optind == argc) {
                    if (opterr) {
                        /* 1003.2 specifies the format of this message.  */
                        fprintf(stderr, "%s: option requires an argument -- %c\n", argv[0], c);
                    }
                    optopt = c;
                    if (optstring[0] == ':')
                        c = ':';
                    else
                        c = BAD_OPTION;
                } else
                    /* We already incremented `optind' once;
                   increment it again when taking next ARGV-elt as argument.  */
                    optarg = argv[optind++];
                nextchar = NULL;
            }
        }
        return c;
    }
}

int getopt_long(int argc, char* const* argv, const char* options, const struct option* long_options, int* opt_index)
{
    return _getopt_internal(argc, argv, options, long_options, opt_index, 0);
}
#endif

int main(int argc, char** argv)
{
    /* Parse args */
    while (1) {
        int idx = 0;
        int c = getopt_long(argc, argv, "fhusr:", longOptionsG, &idx);

        if (c == -1) {
            /* End of options */
            break;
        }

        switch (c) {
            case 'f':
                forceG = 1;
                break;
            case 'h':
                uriStyleG = OBS_URI_STYLE_VIRTUALHOST;
                break;
            case 'u':
                protocolG = OBS_PROTOCOL_HTTP;
                break;
            case 's':
                showResponsePropertiesG = 1;
                break;
            case 'r': {
                const char* v = optarg;
                retriesG = 0;
                while (*v) {
                    retriesG *= 10;
                    retriesG += *v - '0';
                    v++;
                }
                break;
            }
            default:
                fprintf(stderr, "\nERROR: Unknown option: -%c\n", c);
                /* Usage exit */
                usageExit(stderr);
        }
    }

    /* The first non-option argument gives the operation to perform */
    if (optind == argc) {
        fprintf(stderr, "\n\nERROR: Missing argument: command\n\n");
        usageExit(stderr);
    }

    const char* command = argv[optind++];  //lint !e52

    if (!strcmp(command, "help")) {
        fprintf(stdout,
            "\ns3 is a program for performing single requests "
            "to Huawei S3.\n");
        usageExit(stdout);
    }

    if (argc < 6) {
        fprintf(stderr, "\n need five parameter, excuter type, prefix, address, ak, sk");
        return -1;
    }

    if (!strcmp(command, "delete")) {
        if (optind == argc) {
            fprintf(stderr, "\nERROR: Missing parameter: bucket or bucket/key\n");
            usageExit(stderr);
        }

        hostName = argv[optind + 1] + strlen("address=");
        accessKeyIdG = argv[optind + 2] + strlen("ak=");
        secretAccessKeyG = argv[optind + 3] + strlen("sk=");

        char* val = argv[optind];
        int hasSlash = 0;
        while (*val) {
            if (*val++ == '/') {
                hasSlash = 1;
                break;
            }
        }
        if (hasSlash) {
            char* slash = argv[optind];
            while (*slash && (*slash != '/')) {
                slash++;
            }
            *slash++ = 0;
            ListObjectsCallBackData objectsData;
            int i = 0;
            for (; i < 1000; i++) {
                memset(objectsData.bucket[i], '\0', 1000);
            }
            list_obs_objects(argv[optind], slash, &objectsData);

            for (i = 0; i < objectsData.objectsNum; i++) {
                delete_obs_objects(argv[optind], objectsData.bucket[i]);
            }
        } else {
            /* null */
        }
    }

    if (NULL != pCAInfo) {
        free(pCAInfo);
    }

    return 0;
}
//lint +e26 +e30 +e31 +e42 +e48 +e50 +e63 +e64 +e78 +e86 +e101 +e119 +e129 +e142 +e144 +e156 +e409 +e438 +e505 +e516 +e515 +e522 +e525 +e528 +e529 +e530 +e533 +e534 +e539 +e546 +e550 +e551 +e560 +e565 +e574 +e578 +e601
