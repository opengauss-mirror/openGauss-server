#!/bin/bash
#
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
# 
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
# 
#          http://license.coscl.org.cn/MulanPSL2
# 
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ---------------------------------------------------------------------------------------
# 
# sh
#        shell script for pgxc_find_unconsistency
# 
# IDENTIFICATION
#        contrib/pgxc_find_unconsistency/pgxc_find_unconsistency.sh
# 
# ---------------------------------------------------------------------------------------

set -e

#this shell is to check up the transaction coherence,it will return the different xid of committed and aborted.              

# the name of db that you will connect.                                                                                      
dbname=$1                                                                                                                    
                                                                                                                             
# the number of port that you will connect.                                                                                  
portname=$2                                                                                                                  
                                                                                                                             
if [ -z $1 ]; then                                                                                                           
	dbname=postgres                                                                                                      
fi                                                                                                                           
if [ -z $2 ]; then                                                                                                           
	portname=5432                                                                                                        
fi                                                                                                                           
                                                                                                                             
#get the xid's status of all  datanodes.                                                                                     
gsql -d $dbname -p $portname -c "select pgxc_parse_clog()" -o run.out                                                               
                                                                                                                             
#get the committed xid from the run.out.                                                                                     
grep "COMMITTED" run.out | awk -F, '{print $1}' | awk -F'(' '{print $2}' > committed.xid                                     
                                                                                                                             
#get the aborted xid from the run.out.                                                                                       
grep "ABORTED" run.out | awk -F, '{print $1}' | awk -F'(' '{print $2}' > aborted.xid                                         
                                                                                                                                                                                             
if [ -f xillist ]                                                                                                            
then                                                                                                                         
	rm xillist                                         
fi

#get the intersection of committed xid and aborted xid.   
grep -F -f committed.xid aborted.xid | sort | uniq > xillist
                                                                                                                             
if [ -s xillist ]                                                                                                            
then                                                                                                                         
	cat xillist                                                                                                          
	echo "The above number is the clashed xid"                                                                           
else                                                                                                                         
	echo "Congratulations! No clashed xid!"                                                                              
fi                                                                                                                           
                                                                                                                             
#remove the medial file                                                                                                      
rm  committed.xid aborted.xid                                                                                                
