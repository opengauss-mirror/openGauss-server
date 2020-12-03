source ./common.sh


function usage()
{
    echo "usage: $0 [option]
         --help
         --stop_agent
         --stop_detector
         --stop_all
         "
}


function stop_agent()
{
    cat ${CURRENT_DIR}/${AGENT_PID} | xargs kill -9
}


function stop_detector()
{
    local user=""
    local host=""
    local detector_path=""
    local password=""
    local port=22

    read -p "please input the user of detector: " user
    read -p "please input the host of detector: " host
    read -p "please input the path of detector: " detector_path
    read -s -p "please input the password of ${user}@${host}: " password

expect <<-EOF
    spawn ssh ${host} -p ${port} -l ${user}
    expect {
       "(yes/no)?" {
           send "yes\r"
           expect "*assword:"
           send "${password}\r"
       }
       "*assword:" {
           send "${password}\r"
       }
       "Last login:" {
           send "\r"
       }

    }
    send "\r"
    expect "*]*"
    send "cat ${detector_path}/${BASENAME}/${MONITOR_PID} | xargs kill -9\r"
    expect "*]*"
    send "cat ${detector_path}/${BASENAME}/${SERVER_PID} | xargs kill -9\r"
    expect "*]*"
    send "exit\r"
    expect eof
EOF
}


function stop_all()
{
    stop_agent
    stop_detector
}


function main()
{
    if [ $# -ne 1 ]; then
        usage
        exit 1
    fi

    case "$1" in
        --help)
            usage
            break
            ;;
        --stop_agent)
            stop_agent
            break
            ;;
        --stop_detector)
            stop_detector
            break
            ;;
        --stop_all)
            stop_all
            break
            ;;
        *)
            echo "unknown arguments"
            ;;
    esac
}


main $@
