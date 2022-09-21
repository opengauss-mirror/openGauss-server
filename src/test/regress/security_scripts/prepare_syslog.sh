#!/bin/bash

current_user=$(whoami)
if [ "$current_user" != "root" ]; then
    sudo -l -n -U $current_user | grep "(ALL) NOPASSWD"
    if [ $? -ne 0 ] 
    then
        echo "please config sudo privilege for ${current_user} for rsyslog operation"
        exit 1
    fi
fi

sudo cp /etc/rsyslog.conf /etc/rsyslog.conf_bk
sudo sed  -i '/^local0*/d' /etc/rsyslog.conf
echo "local0.*                                                /var/log/localmessages" | sudo tee -a /etc/rsyslog.conf
sudo rm -rf /var/log/localmessages
sudo touch /var/log/localmessages
sudo chmod a+r /var/log/localmessages 

if [ `which systemctl` ];then
    sudo systemctl restart rsyslog
else
    echo "no systemctl service"
    exit 1
fi

