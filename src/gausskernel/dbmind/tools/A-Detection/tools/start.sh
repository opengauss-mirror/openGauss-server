source ./common.sh


function usage()
{
    echo "usage: $0 [option]
         --help
         --deploy_code
         --start_agent
         --start_detector
         --start_all
         "
}


function start_agent()
{
    cd ${CURRENT_DIR}
    nohup python main.py -r agent > /dev/null 2>&1 &
}


function start_detector()
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
    send "cd ${detector_path}/${BASENAME}\r"
    expect "*]*"
    send "nohup python main.py -r detector > /dev/null 2>&1 &\r"
    expect "*]*"
    send "exit\r"
    expect eof
EOF

}


function deploy_code()
{
    local user=""
    local host=""
    local detector_path=""

    read -p "please input the user of detector: " user
    read -p "please input the host of detector: " host
    read -p "please input the path of detector: " detector_path
    read -s -p "please input the password of ${user}@${host}: " password

expect <<-EOF
    spawn scp -r ${CURRENT_DIR} ${user}@${host}:${detector_path}
    expect {
        "(yes/no)?" {
            send "yes\r"
            expect "*assword:"
            send "${password}\r"
        }
        "*assword" {
            send "${password}\r"
        }
}
    expect eof
EOF


}


function start_all()
{
    start_agent
    start_detector
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
        --start_agent)
            start_agent
            break
            ;;
        --start_detector)
            start_detector
            break
            ;;
        --deploy_code)
            deploy_code
            break
            ;;
        --start_all)
            start_all
            break
            ;;
        *)
            echo "unknown arguments"
            ;;
    esac
}


main $@
