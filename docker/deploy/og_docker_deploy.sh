#!/bin/bash

declare current_ip=
declare current_role=
declare current_node=
declare config_file=
declare current_volpath=
declare current_mapport=
declare other_nodeips=()

ROLE_PRIMARY=primary
ROLE_STANDBY=standby

function print_help() {
    echo "Usage: $0 [OPTION]
    -h|--help                         show help information
    -m|--mode                         db instance role, primary or standby
    -node|--nodename                  current docker name
    -f|--config_file                  docker instance config file
    "
}

if [ $# = 0 ]; then
    echo "missing option"
    print_help
    exit 1
fi

while [ $# -gt 0 ]; do
    case "$1" in
    -h | --help)
        print_help
        exit 1
        ;;
    -m | --mode)
        if [ "$2"X = X ]; then
            echo "no instance mode, such as: primary | standby"
            exit 1
        fi
        current_role=$2
        shift 2
        ;;
    -node | --nodename)
        if [ "$2"X = X ]; then
            echo "no given current node name"
            exit 1
        fi
        current_node=$2
        shift 2
        ;;
    -f | --config_file)
        if [ "$2"X = X ]; then
            echo "no given config file path"
            exit 1
        fi
        config_file=$2
        shift 2
        ;;
    *)

        echo "./build.sh --help or ./build.sh -h"
        exit 1
        ;;
    esac
done

if [ "$current_role" == "" ] || [ "$current_node" == "" ] || [ "$config_file" == "" ]; then
    echo "-m|-node|-f options is needed."
    exit 1
fi

if [ "$current_role" != "$ROLE_PRIMARY" ] && [ "$current_role" != "$ROLE_STANDBY" ]; then
    echo "-m must be primary or standby"
    exit 1
fi

if [ ! -f $config_file ]; then
    echo "config file [$config_file] is not exist"
    exit 1
fi

while IFS='=' read -r key value; do
    if [[ ! $key =~ ^# ]]; then
        key=$(echo $key | xargs | tr -d '\r')
        value=$(echo $value | xargs | tr -d '\r')
        declare "$key"="$value"
    fi
done <${config_file}

function check_params()
{
    ## check image if exist
    imagename=$(echo "$opengauss_image" | cut -d ':' -f 1)
    imageversion=$(echo "$opengauss_image" | cut -d ':' -f 2)
    if ! docker images | grep -w $imagename | grep -w $imageversion; then
        echo "$opengauss_image is not exist"
        exit 1
    fi

    ## check network if exist
    if [ "$(docker network ls | awk '{print $2}' | grep -w ${dockernetwork})" == "" ]; then
        echo "docker network $dockernetwork is not exist"
        exit 1
    fi

}

function init_instance() {
    docker run --name ${current_node} \
        --net=${dockernetwork} \
        -p ${current_mapport}:5432 \
        --ip ${current_ip} \
        -d -e GS_PASSWORD=${ogpassword} \
        -v ${current_volpath}:/var/lib/opengauss \
        ${opengauss_image}

    echo "waiting init instance..."
    sleep 30

    if [ "$current_role" == "${ROLE_STANDBY}" ]; then
        docker stop ${current_node}
        docker rm ${current_node}

        docker run --name ${current_node} \
            --net=${dockernetwork} \
            -p ${current_mapport}:5432 \
            --ip ${current_ip} \
            -d -e GS_PASSWORD=${ogpassword} \
            -v ${current_volpath}:/var/lib/opengauss \
            -it --entrypoint /bin/bash \
            ${opengauss_image}
    fi
}

function config_instance() {
    docker exec ${current_node} su - omm -c "gs_guc reload -D /var/lib/opengauss/data -c 'remote_read_mode=off'"
    docker exec ${current_node} su - omm -c "gs_guc reload -D /var/lib/opengauss/data -c 'replication_type=1'"
    docker exec ${current_node} su - omm -c "gs_guc reload -D /var/lib/opengauss/data -c \"application_name='${current_node}'\""
    docker exec ${current_node} su - omm -c "gs_guc reload -D /var/lib/opengauss/data -h 'host all omm ${current_ip}/32 trust'"
    declare replindex=0
    for index in "${!other_nodeips[@]}"; do
        element=$(echo ${other_nodeips[$index]} | xargs)

        docker exec ${current_node} su - omm -c "gs_guc reload -D /var/lib/opengauss/data -c \"replconninfo$(($index + 1))='localhost=${current_ip} localport=5433 localheartbeatport=5436 localservice=5437 remotehost=${element} remoteport=5433 remoteheartbeatport=5436 remoteservice=5437'\""
        docker exec ${current_node} su - omm -c "gs_guc reload -D /var/lib/opengauss/data -h 'host all omm ${element}/32 trust'"

    done
}

function build_standby_instance() {
    if [ "$current_role" != "${ROLE_STANDBY}" ]; then
        return 0
    fi

    docker exec ${current_node} su - omm -c "gs_ctl build -D /var/lib/opengauss/data -M standby"

    docker stop ${current_node}
    docker rm ${current_node}
    docker run --name ${current_node} \
        --net=${dockernetwork} \
        -p ${current_mapport}:5432 \
        --ip ${current_ip} \
        -d -e GS_PASSWORD=${ogpassword} \
        -v ${current_volpath}:/var/lib/opengauss \
        ${opengauss_image} -M standby
}

function query_result() {
    sleep 10
    docker exec ${current_node} su - omm -c "gs_ctl query -D /var/lib/opengauss/data"
}

function parse_params() {
    IFS=','
    read -ra name_arr <<<"$node_names"
    read -ra ip_arr <<<"$node_ips"
    read -ra vol_arr <<<"$docker_vols"
    read -ra mapport_arr <<<"$server_port_maps"
    unset IFS

    for index in "${!name_arr[@]}"; do
        element=$(echo ${name_arr[$index]} | xargs)
        if [ "$current_node"X == "$element"X ]; then
            current_ip=$(echo ${ip_arr[$index]} | xargs | tr -d '\r')
            current_mapport=$(echo ${mapport_arr[$index]} | xargs | tr -d '\r')
            current_volpath=$(echo ${vol_arr[$index]} | xargs | tr -d '\r')
        else
            other_nodeips+=($(echo ${ip_arr[$index]} | xargs))
        fi
    done

    echo "curent instance: name $current_node, ip ${current_ip}, role ${current_role}"

    if [ "$ogpassword" == "" ]; then
        echo "Please enter a password with at least 8-16 digits containing numbers, letters, and special characters: " 
        read -s ogpassword
        if [[ "$ogpassword" =~  ^(.{8,}).*$ ]] &&  [[ "$ogpassword" =~ ^(.*[a-z]+).*$ ]] && [[ "$ogpassword" =~ ^(.*[A-Z]).*$ ]] &&  [[ "$ogpassword" =~ ^(.*[0-9]).*$ ]] && [[ "$ogpassword" =~ ^(.*[#?!@$%^&*-]).*$ ]]; then
            echo "The supplied ogpassword is meet requirements."
        else
            echo "Please Check if the password contains uppercase, lowercase, numbers, special characters, and password length(8).
                  At least one uppercase, lowercase, numeric, special character."
            exit 1
        fi
    fi
}

check_params
parse_params
init_instance
config_instance
build_standby_instance
query_result
