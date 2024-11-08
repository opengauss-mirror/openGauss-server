#include <arpa/inet.h>
#include "postgres.h"
#include "funcapi.h"
#include "commands/extension.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "getaddrinfo.h"

#include "gms_inaddr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gms_inaddr_get_host_address);
PG_FUNCTION_INFO_V1(gms_inaddr_get_host_name);

#define MAXLINE 8192
#define ADDRESSSIZE 40

Datum
gms_inaddr_get_host_address(PG_FUNCTION_ARGS)
{
	struct addrinfo *gai_result = NULL, *gai = NULL;
    	int ret = -1;
	char result[ADDRESSSIZE] = {0};
    	
	char* hostname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	if(PG_ARGISNULL(0)){
		char hostname[256] = {0};

    		(void)gethostname(hostname, 255);
            	ereport(WARNING,
                	(errcode(ERRCODE_WARNING),
                    		errmsg("hostname %s!", hostname)));
	}

	ret = getaddrinfo(hostname, NULL, NULL, &gai_result);
	if (ret != 0) {
		ereport(ERROR,
    			(errcode(ERRCODE_CONFIG_FILE_ERROR),
        			errmsg(
            				"could not translate host name \"%s\" to address: %s", hostname, gai_strerror(ret))));
	}
	for (gai = gai_result; gai; gai = gai->ai_next) {
		errno_t rt;
    		if (gai->ai_addr->sa_family == AF_INET) {
			struct sockaddr_in* h = (struct sockaddr_in*)gai->ai_addr;
			char* address = inet_ntoa(h->sin_addr);
			rt = strcpy_s(result, ADDRESSSIZE, address);
			securec_check(rt, "\0", "\0");
            		break;
    		}
		#ifdef HAVE_IPV6
    		else if (gai->ai_addr->sa_family == AF_INET6) {
			struct sockaddr_in6* h = (struct sockaddr_in6*)gai->ai_addr;
			inet_net_ntop(AF_INET6, &(h)->sin6_addr, 128, result, ADDRESSSIZE);
            		break;
    		}
		#endif
	}


	
	if (gai_result != NULL)
		freeaddrinfo(gai_result);

	text* result_text = cstring_to_text(result);
	PG_RETURN_TEXT_P(result_text);
}


Datum
gms_inaddr_get_host_name(PG_FUNCTION_ARGS){
	char* ip_address = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if(PG_ARGISNULL(0)){
		ip_address = "127.0.0.1";
	}

	struct sockaddr_in addr;
    	addr.sin_family = AF_INET;
    	addr.sin_addr.s_addr = inet_addr(ip_address);
	
    	char buf[MAXLINE];
	
    	int rc = getnameinfo((struct sockaddr *)&addr,sizeof(struct sockaddr_in),buf,MAXLINE,NULL,0,NI_NAMEREQD);
	

	if(rc != 0){
		elog(ERROR,
                        "error happen when fetch hostname: %s",
			gai_strerror(rc));
	}	

    	text* result_text = cstring_to_text(buf);

    	PG_RETURN_TEXT_P(result_text);
}
