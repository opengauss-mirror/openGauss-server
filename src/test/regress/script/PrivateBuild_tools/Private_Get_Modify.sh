#!/bin/bash
tools_path=/home/PrivateBuild_tools
agent_path=/usr1/gauss_jenkins/jenkins/workspace/openGauss/
log_path=/home/log
mkdir -p ${log_path}

function execute_flag(){
	if [ $? -ne "0" ];then
        echo "-----------execute shell command failed!!----------------"
		exit 1
	fi
}
ps -ef |grep  PrivateBuild_tools|grep -v grep
ps -ef |grep git|grep -v grep
echo "<------------------------------------------------------get modify file start-------------------------------------------------------------->"
cd $agent_path
# clean the crashed index.lock file
test -f $agent_path/.git/index.lock && rm -rf $agent_path/.git/index.lock

echo "-----------checkout ${iSourceTargetBranch} begin...----------------"
date
git reset --hard   1>>$log_path/tmplog 2>>$log_path/pullerror
git checkout -f ${iSourceTargetBranch}    1>>$log_path/tmplog 2>>$log_path/pullerror
execute_flag
date
echo "-----------checkout ${iSourceTargetBranch} end.    ----------------"


echo "-----------git reset --hard ${iSourceTargetBranch} begin...----------------"
date
git clean -df    1>>$log_path/tmplog 2>>$log_path/pullerror
git reset --hard origin/${iSourceTargetBranch}    1>>$log_path/tmplog 2>>$log_path/pullerror
execute_flag
date
echo "-----------git reset --hard ${iSourceTargetBranch} end.    ----------------"


echo "--------------git pull --no-edit ${iSourceTargetRepoSshUrl} ${iSourceTargetBranch} begin...-------------"
date
git status    >>  $log_path/pullerror
git pull --no-edit ${iSourceTargetRepoSshUrl} ${iSourceTargetBranch}  1>>$log_path/tmplog 2>>$log_path/pullerror
if [ $? -ne "0" ];then
     git clean -df   >>  $log_path/tmplog
     git reset --hard   >>  $log_path/tmplog
     git pull --no-edit ${iSourceTargetRepoSshUrl} ${iSourceTargetBranch}  1>>$log_path/tmplog 2>>$log_path/pullerror
     if [ $? -ne "0" ];then
         git  reset --hard origin/${iSourceTargetBranch}  >>  $log_path/tmplog
         echo "--------------git pull --no-edit ${iSourceTargetRepoSshUrl} ${iSourceTargetBranch} Failed  -------------"
         exit 1
     fi
fi
# use git-lfs download trunk code
git-lfs pull >>  $log_path/tmplog
date
echo "--------------git pull --no-edit ${iSourceTargetRepoSshUrl} ${iSourceTargetBranch} end.    -------------"

git reset --hard

echo "--------------git pull --no-edit ${iSourceSourceRepoSshUrl} ${iSourceSourceBranch} begin...-------------"
date
git pull --no-edit ${iSourceSourceRepoSshUrl} ${iSourceSourceBranch}  1>$log_path/build_incr_file 2>>$log_path/pullerror
if [ $? -ne "0" ];then
    git status  >>  $log_path/pullerror
    git pull --no-edit ${iSourceSourceRepoSshUrl} ${iSourceSourceBranch}  1>$log_path/build_incr_file 2>>$log_path/pullerror
    if [ $? -ne "0" ];then
        git  reset --hard origin/${iSourceTargetBranch}  >>  $log_path/tmplog
        echo "--------------git pull --no-edit ${iSourceSourceRepoSshUrl} ${iSourceSourceBranch} Failed  -------------"
        exit 1
    fi
fi
date
echo "--------------git pull --no-edit ${iSourceSourceRepoSshUrl} ${iSourceSourceBranch} end.    -------------"

cd $agent_path

echo "<------------------------------------------------------get modify file end  -------------------------------------------------------------->"

