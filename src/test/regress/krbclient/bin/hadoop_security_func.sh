#!/bin/bash 

##########################################################
#    Global param for extern caller
##########################################################

#ticket path,used for application get security_auth_ticket_path
security_auth_ticket_path=""


##########################################################
#    Global param for inner caller
##########################################################
# shell path
sec_this_shell_path=""
# ticket file number
sec_ticket_file_num=0
# ticket file max number
sec_ticket_file_max_num=999
# ticket file name max lenth
sec_ticket_file_max_lenth=255
#ticket inner path
sec_inner_auth_ticket_path=""
#kinit binary file name
sec_kinit_bin="kinit"
#kdestroy binary file name
sec_kdestroy_bin="kdestroy"
#kinit binary file path
sec_seccmd_path=""
#kinit binary file path
sec_sechome_path=""
#ticket cache defaulte root dir
sec_ticket_cache_default_root_dir=/tmp



##########################################################
#    Description: sec_get_this_shell_path
#    Parameter:   None
#    Return:      None
#                 
##########################################################
function sec_get_this_shell_path()
{
    local source_path="$(dirname "${BASH_SOURCE-$0}")"
    cd "${source_path}"
    sec_this_shell_path="$(readlink -e "$(pwd)")"
    cd - > /dev/null 2>&1
}

##########################################################
#    Description: log function
#    Parameter:   None
#    Return:      None
##########################################################
function sec_seclog()
{
    echo -e "[`date +%y-%m-%d\" \"%T`]: $*"
}


##########################################################
#    Description: get krb5 home path
#    Parameter:   none
#                 global param sec_seccmd_path
#    Return:      0: success
#                 1: failed
##########################################################
function sec_get_security_home_path()
{
    local krb5_home=""
    
    ############################################
    ## check krb5 env, if not set use path or HOME env
    ############################################   
    if [ -n "${KRB_HOME}" ];then
        krb5_home="${KRB_HOME}"
    elif [ -n "${KRB5_HOME}" ];then
        krb5_home="${KRB5_HOME}"
    elif [ -d "${HOME}/kerberos" ];then
        krb5_home="${HOME}/kerberos"    
    fi    
    
    if [ ! -d "${krb5_home}" ];then
        return 1
    fi
    sec_sechome_path="${krb5_home}"
    return 0
}

##########################################################
#    Description: get krb5 config path and set config env
#    Parameter:   none
#                 
#    Return:      0: success
#                 1: failed
##########################################################
function sec_get_security_config_path()
{
    local krb5_home=""
    local krb5_config=""
    
    ############################################
    ## get krb5 home path
    ############################################      
    sec_get_security_home_path
    if [ 0 -eq $? ];then
        krb5_home="${sec_sechome_path}" 
    fi
    
    if [ -n "${KRB5_CONFIG}" ];then
        krb5_config="${KRB5_CONFIG}"
    elif [ -n "${krb5_home}" ];then
        krb5_config="${krb5_home}/var/krb5kdc/krb5.conf"
        export KRB5_CONFIG="${krb5_config}"
    fi
    
    if [ -z "${krb5_config}" ];then
        return 1
    fi 
    
    return 0
    
}

##########################################################
#    Description: get krb5 command path
#    Parameter:   $1 krb5 command
#                 global param sec_seccmd_path
#    Return:      0: success
#                 1: failed
##########################################################
function sec_get_security_cmd_path()
{
    local cmd_bin="$1"
    local krb5_home=""
    local cmd_path=""
    
    ############################################
    ## get krb5 home path
    ############################################      
    sec_get_security_home_path
    if [ 0 -eq $? ];then
        krb5_home="${sec_sechome_path}" 
    fi
            
    if [ -n "${krb5_home}" ];then
        cmd_path="${krb5_home}/bin/${cmd_bin}"
    else
        cmd_path="$(which "${cmd_bin}")"
    fi

    ############################################
    ## check cmd file exist or not 
    ############################################     
    if [ ! -f "${cmd_path}" ];then        
        return 1            
    fi  
    
    ############################################
    ## set global param 
    ############################################        
    sec_seccmd_path="${cmd_path}"
    return 0  
}

##########################################################
#    Description: generate ticket path
#    Parameter:   $1 principal name
#    Return:      0: success
#                 1: failed
##########################################################
function sec_gen_ticket_path()
{
    local principal_name="$1"
    local cache_root_dir="${HOME}"
    
    ############################################
    ##  check if all param input 
    ############################################
    if [ -z "${principal_name}" ];then
        sec_seclog "principal name must input."
        return 1         
    fi
    
    ############################################
    ##  check if HOME dir is set, if not set use default,then check if dir exist
    ############################################
    if [ -z "${cache_root_dir}" ];then
        cache_root_dir="${sec_ticket_cache_default_root_dir}"
    fi
    
    if [ ! -d "${cache_root_dir}" ];then
        sec_seclog "cache root dir ${cache_root_dir} not exist."
        return 1          
    fi
    
    
    ############################################
    ##  generate cache file name 
    ############################################    
    local script_name="$(echo $(basename $0) | sed 's/.sh$//g')"    
    local user_name="$(echo "$1" | awk -F/ '{print $1}')"
    local script_pid="$$"
    local file_num=${sec_ticket_file_num}
    local cache_name="${script_name}_${user_name}_${script_pid}_${file_num}.cache"
    local cache_name_len=$(echo "${cache_name}" | awk '{print length($0)}')
    
    ############################################
    ##  if cache file name too long use short name 
    ############################################  
    if [ $cache_name_len -ge ${sec_ticket_file_max_lenth} ];then
        cache_name="${user_name}_${script_pid}_${file_num}.cache"
    fi
    
    
    ############################################
    ##  set global param
    ############################################    
    sec_inner_auth_ticket_path="${cache_root_dir}/cache/${cache_name}"
    ((sec_ticket_file_num++))
    if [ ${sec_ticket_file_num} -gt ${sec_ticket_file_max_num} ];then
        sec_ticket_file_num=0
    fi
    return 0
    
}

##########################################################
#    Description: check ticket path if not exist create
#    Parameter:   $1 ticket path
#    Return:      0: success
#                 1: failed
##########################################################
function sec_check_ticket_path()
{
    local ticket_path="${1}"
    local ret=0
    
    ############################################
    ##  check if param input 
    ############################################    
    if [ -z "${ticket_path}" ];then
        sec_seclog "ticket path must input."
        return 1        
    fi

    ############################################
    ##  create dir for ticket if path not exist 
    ############################################    
    if [ ! -d "${ticket_path}" ];then
        mkdir -p "${ticket_path}"
        ret=$?
        if [ 0 -ne ${ret} ];then
            sec_seclog "ticket path must input."
            return 1              
        fi
        return 0
    fi
    
    return 0
    
    
}

##########################################################
#    Description: generate ticket file from keytab
#    Parameter:   $1 principal name 
#                 $2 keytab file
#                 $3 ticket file path
#    Return:      0: success
#                 1: failed
##########################################################
function sec_gen_ticket_file_from_keytab()
{
    local principal_name="$1"
    local principal_key="$2"   
    local ticket_cache_path="$3" 
    
    local krb5_kinit_cmd=""
    local ret=0

    ############################################
    ## get kinit command file path
    ############################################      
    sec_get_security_cmd_path "${sec_kinit_bin}"
    if [ 0 -ne $? ];then
        sec_seclog "command ${sec_kinit_bin} binary file not exist."
        return 1         
    fi
    krb5_kinit_cmd="${sec_seccmd_path}"    
    
    ############################################
    ## delete old ticket file first
    ############################################     
    if [ -f "${ticket_cache_path}" ];then
        rm -f "${ticket_cache_path}"
    fi
    
    ############################################
    ## excute kinit cmd to generate ticket cache
    ############################################     
    "${krb5_kinit_cmd}" -k -t "${principal_key}" -c "${ticket_cache_path}" "${principal_name}"
    ret=$?
    if [ 0 -ne ${ret} ] || [ ! -f "${ticket_cache_path}" ];then
        return 1
    fi
    
    return 0
}

##########################################################
#    Description: generate ticket file from passwd
#    Parameter:   $1 principal name   
#                 $2 passwd
#                 $3 ticket file path
#    Return:      0: success
#                 1: failed
##########################################################
function sec_gen_ticket_file_from_passwd()
{
    local principal_name="$1"
    local principal_key="$2"   
    local ticket_cache_path="$3" 
    
    local krb5_kinit_cmd=""
    local ret=0
    
    ############################################
    ## get kinit command file path
    ############################################      
    sec_get_security_cmd_path "${sec_kinit_bin}"
    if [ 0 -ne $? ];then
        sec_seclog "command ${sec_kinit_bin} binary file not exist."
        return 1         
    fi
    krb5_kinit_cmd="${sec_seccmd_path}"        
    
    ############################################
    ## delete old ticket file first
    ############################################     
    if [ -f "${ticket_cache_path}" ];then
        rm -f "${ticket_cache_path}"
    fi
    
    ############################################
    ## excute init script to generate ticket cache
    ############################################   
    "${sec_this_shell_path}/security_krb5_init_from_pwd.sh" "${krb5_kinit_cmd}" "${principal_name}" "${principal_key}" "${ticket_cache_path}"
    ret=$?
    if [ 0 -ne ${ret} ] || [ ! -f "${ticket_cache_path}" ];then
        return 1
    fi
    
    return 0    
}

##########################################################
#    Description: generate ticket file
#    Parameter:   $1 authentication mode(keytab/passwd)
#                 $2 principal name
#                 $3 keytab file or passwd for principal
#                 $4 ticket file path
#    Return:      0: success
#                 1: failed
##########################################################
function sec_gen_ticket_file()
{
    local auth_mode="$1"
    local principal_name="$2"
    local principal_key="$3"   
    local ticket_cache_path="$4"      
    local kr5b_kinit=""
    local ret=0
    
    ############################################
    ## inner funciton do not need to check param,check krb5 env and command binary file
    ############################################
    sec_get_security_cmd_path "${sec_kinit_bin}"
    if [ 0 -ne $? ];then
        sec_seclog "command ${sec_kinit_bin} binary file not exist."
        return 1         
    fi    
       
    sec_get_security_config_path    
    if [ 0 -ne $? ];then
        sec_seclog "get krb5 config path failed."
        return 1
    fi
    
    ############################################
    ##  generate keytab file 
    ############################################
    if [ "keytab" ==  "${auth_mode}" ];then
        sec_gen_ticket_file_from_keytab "${principal_name}" "${principal_key}" "${ticket_cache_path}"
        ret=$?
        if [ 0 -ne ${ret} ];then
            sec_seclog "generate ticket file(${ticket_cache_path}) for ${principal_name} from ${principal_key} failed."
            return 1             
        fi
        return 0
       
    fi

    if [ "passwd" ==  "${auth_mode}" ];then
        sec_gen_ticket_file_from_passwd "${principal_name}" "${principal_key}" "${ticket_cache_path}"
        ret=$?
        if [ 0 -ne ${ret} ];then
            sec_seclog "generate ticket file(${ticket_cache_path}) for ${principal_name} from passwd failed."
            return 1             
        fi
        return 0
       
    fi
    
    sec_seclog "generate ticket file(${ticket_cache_path}) for ${principal_name} from ${principal_key} failed: mode(${auth_mode}) error."
    return 1
           
}

##########################################################
#    Description: check authentication enable
#    Parameter:   $1 the principal name
#    Return:      0: authentication disable
#                 1: authentication enable
##########################################################
function is_authentication_enable()
{
    if [ "kerberos" == "${HADOOP_SECURITY_AUTHENTICATION}" ];then
        return 1
    else
        return 0
    fi
}

##########################################################
#    Description: create authentication ticket
#    Parameter:   $1 authentication mode(keytab/passwd)
#                 $2 principal name
#                 $3 keytab file or passwd for principal
#    Global Parameter: security_auth_ticket_path
#                      output param, used to return ticket path to caller 
#    Return:      0: create success
#                 1: create failed
##########################################################
function init_authentication_ticket()
{
    local auth_mode="$1"
    local principal_name="$2"
    local principal_keytab="$3"   
    local ticket_cache_path=""     
    
    
    
    ############################################
    ##  check if all param input 
    ############################################
    if [ -z "${auth_mode}" ];then
        sec_seclog "authentication mode must input."
        return 1
    fi
        
    if [ -z "${principal_name}" ];then
        sec_seclog "principal name must input."
        return 1
    fi
    
    if [ -z "${principal_keytab}" ];then
        sec_seclog "principal keytab must input."
        return 1
    fi   

    ############################################
    ##  check authentication mode is valid 
    ############################################
    
    if [ "keytab" != "${auth_mode}" ] && [ "passwd" != "${auth_mode}" ];then
        sec_seclog "authentication mode must be keytab or passwd"
        return 1
    fi
    
    #check keytab file is exist
    if [ "keytab" == "${auth_mode}" ] && [ ! -f "${principal_keytab}" ];then
        sec_seclog "principal keytab file ${principal_keytab} for ${principal_name} not exist"
        return 1
    fi

    ############################################
    ##  generate ticket path
    ############################################
    sec_gen_ticket_path "${principal_name}"
    if [ 0 -ne $? ];then
        sec_seclog "generate ticket path failed."
        return 1        
    fi
    ticket_cache_path="${sec_inner_auth_ticket_path}"

    ############################################
    ##  check ticket path
    ############################################
    sec_check_ticket_path "$(dirname "${ticket_cache_path}")"
    if [ 0 -ne $? ];then
        sec_seclog "generate ticket path failed."
        return 1        
    fi   
    
    ############################################
    ##  generate ticket file
    ############################################    
    sec_gen_ticket_file "${auth_mode}" "${principal_name}" "${principal_keytab}" "${ticket_cache_path}"
    if [ 0 -ne $? ];then
        sec_seclog "generate ticket file failed."
        return 1        
    fi   
    
    #set global path
    security_auth_ticket_path="${ticket_cache_path}"
    return 0   
    
    
    
}


##########################################################
#    Description: destroy authentication ticket
#    Parameter:   $1 the ticket path
#    Return:      0: success
#                 1: failed
##########################################################
function destroy_authentication_ticket()
{
    local ticket_path="$1"
    local kr5b_kinit=""

    
    local krb5_kdestroy_cmd=""
        
    ############################################
    ##  check if all param input 
    ############################################
    if [ -z "${ticket_path}" ];then
        sec_seclog "ticket path must input."
        return 1
    fi 
    
    ############################################
    ##  check if file exist,if not do not need del
    ############################################    
    if [ ! -f "${ticket_path}" ];then        
        return 0
    fi       
    
    ############################################
    ## check krb5 env and krb5 command binary file
    ############################################    
    sec_get_security_cmd_path "${sec_kdestroy_bin}"
    if [ 0 -ne $? ];then
        sec_seclog "command ${sec_kdestroy_bin} binary file not exist."
        return 1         
    fi
    krb5_kdestroy_cmd="${sec_seccmd_path}"
        
    sec_get_security_config_path    
    if [ 0 -ne $? ];then
        sec_seclog "get krb5 config path failed."
        return 1
    fi   
    
    ############################################
    ## destroy krb5 ticket
    ############################################            
    "${krb5_kdestroy_cmd}" -q -c "${ticket_path}"
    if [ 0 -eq $? ];then
        return 0
    fi
    
    #force rmv file 
    rm -f "${ticket_path}"
    if [ 0 -ne $? ];then
        return 1
    fi    
    return 0               
}

##########################################################
#    Description: specify authentication ticket to use
#    Parameter:   $1 the ticket path can be null mean clear env
#    Return:      0: success
#                 1: failed
##########################################################
function specify_authentication_ticket()
{
    local ticket_path="${1}"
        

    ############################################
    ##  if param input then check if file exist
    ############################################  
    if [ -n "${ticket_path}" ] && [ ! -f "${ticket_path}" ];then
        sec_seclog "ticket file ${ticket_path} not exist."
        return 1
    fi       
        
    export KRB5CCNAME="${ticket_path}"
    return 0;
}

##########################################################
#    Description: init security running env for caller
#    Parameter:   $1 authentication mode(keytab/passwd)
#                 $2 principal name
#                 $3 keytab file or passwd for principal
#    Return:      0: init success
#                 1: init failed
##########################################################
function init_security_runenv()
{
    local auth_mode="$1"
    local principal_name="$2"
    local principal_keytab="$3"   
    local ticket_cache_path=""   
    local security_enable=1  
    
    ############################################
    ##  if security is not enable return success 
    ############################################    
    is_authentication_enable
    security_enable=$?
    if [ 1 -ne ${security_enable} ];then
        return 0
    fi
        
    ############################################
    ##  check if all param input 
    ############################################
    if [ -z "${auth_mode}" ];then
        sec_seclog "authentication mode must input."
        return 1
    fi
        
    if [ -z "${principal_name}" ];then
        sec_seclog "principal name must input."
        return 1
    fi
    
    if [ -z "${principal_keytab}" ];then
        sec_seclog "principal keytab must input."
        return 1
    fi   

    ############################################
    ##  check authentication mode is valid 
    ############################################
    
    if [ "keytab" != "${auth_mode}" ] && [ "passwd" != "${auth_mode}" ];then
        sec_seclog "authentication mode must be keytab or passwd"
        return 1
    fi
    
    #check keytab file is exist
    if [ "keytab" == "${auth_mode}" ] && [ ! -f "${principal_keytab}" ];then
        sec_seclog "principal keytab file ${principal_keytab} for ${principal_name} not exist"
        return 1
    fi

    ############################################
    ##  create and specify authentication ticket
    ############################################    
    init_authentication_ticket "${auth_mode}" "${principal_name}" "${principal_keytab}"
    if [ 0 -ne $? ];then
        sec_seclog "init security env for ${principal_name} failed."
        return 1        
    fi
    ticket_cache_path="${security_auth_ticket_path}"
    
    specify_authentication_ticket "${ticket_cache_path}"
    if [ 0 -ne $? ];then
        sec_seclog "specify authentication ticket for ${principal_name} failed."
        return 1        
    fi    
    
    return 0
    
}

##########################################################
#    Description: clear security running env 
#    Parameter:   $1 ticket file path
#    Return:      0: clear success
#                 1: clear failed
##########################################################
function clear_security_runenv()
{
    local ticket_path="$1"
        
    ############################################
    ##  if security is not enable return success 
    ############################################    
    is_authentication_enable
    security_enable=$?
    if [ 1 -ne ${security_enable} ];then
        return 0
    fi
            
    ############################################
    ##  check if all param input 
    ############################################
    if [ -z "${ticket_path}" ];then
        sec_seclog "ticket path must input."
        return 1
    fi 
    
    ############################################
    ##  check if file exist,if not do not need del
    ############################################    
    if [ ! -f "${ticket_path}" ];then        
        return 0
    fi   
    
    ############################################
    ##  clear env,then destroy ticket file
    ############################################      
    specify_authentication_ticket ""
    destroy_authentication_ticket "${ticket_path}"
    if [ 0 -ne $? ];then
        sec_seclog "clear security env failed."
        return 1
    fi
    return 0
        
}


##########################################################
#    Description: init security shell
#    Parameter:   $1 the principal name
#    Return:      0: authentication disable
#                 1: authentication enable
##########################################################
function init_this_security_shell()
{
    #get source shell path
    sec_get_this_shell_path
}

init_this_security_shell
