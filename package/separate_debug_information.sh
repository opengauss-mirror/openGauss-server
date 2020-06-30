#!/bin/bash
#############################################################################
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms
# and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ----------------------------------------------------------------------------
# Description  : gs_backup is a utility to back up or restore binary files and parameter files.
#############################################################################

DEPTH=$(pwd)
INSTALL_DIR=$DEPTH/../dest

BIN_DIR=./bin
LIB_DIR=./lib

create_dir()
{
	cd $INSTALL_DIR/$1
	if [ $? -ne 0 ]; then
		echo "change dir to $INSTALL_DIR/$1 failed."
		exit 1
	fi
	list_dir=$(find ./ -name "*" -type d)
	cd $SYMBOLS_DIR
	if [ $? -ne 0 ]; then
		echo "change dir to $SYMBOLS_DIR failed."
		exit 1
	fi
	for t in $list_dir
	do
		if [ "$t"x = "./"x ]; then 
			mkdir $1
			if [ $? -ne 0 ]; then
				echo "mkdir dir to $1 failed."
				exit 1
			fi
			cd $1
		elif [ "$t"x != "./"x ]; then
			mkdir $t; 
			if [ $? -ne 0 ]; then
				echo "mkdir dir to $1 failed."
				exit 1
			fi
		fi
	done	
}

separate_symbol()
{
	INSTALL_DIR=$INSTALL_DIR/$1
	cd $1
	CPTODEST=$CPTODEST/$1
	for x in $(ls)
	do

		if [ -L "$x" ];then
			echo "$x is a link, do not separate symbol"
		elif [[ "$x" = *".py" ]];then
			echo "$x is a script, do not separate symbol"
		elif [[ "$x" = *".dat" ]];then
			echo "$x is a license file, do not separate symbol"
		elif [[ "$x" = *".sh" ]];then
			echo "$x is a shell file, do not separate symbol"
		elif [[ "$x" = *".la" ]];then
			echo "$x is a la file, do not separate symbol"
        elif [[ "$x" = *".crt" ]];then
			echo "$x is a crt file, do not separate symbol"	
		elif [[ "$x" = *".ini" ]];then
			echo "$x is a ini file, do not separate symbol"	
		elif [[ "$x" = *".jar" ]];then
			echo "$x is a jar file, do not separete symbol"
		elif [[ "$x" = "liblwgeom-2.4.so.0" ]];then
			echo "the dynamically link $x do not separate symbol"
		elif [ -f "$x" ];then	
			if [ -x "$x" ]; then
				if [ "$x" != "install-sh"  ]; then
					objcopy --only-keep-debug "$x" "$INSTALL_DIR/${x}.symbol" > /dev/null 2>&1
					objcopy --strip-debug "$x" "$x"_release
					rm  "$x"
					mv "$x"_release "$x"
					objcopy --add-gnu-debuglink="$INSTALL_DIR/${x}.symbol" "$x"
					chmod 755 "$INSTALL_DIR/${x}.symbol"
					mv $INSTALL_DIR/${x}.symbol $CPTODEST
				fi
			elif [[ "$x" = *".so" ]]; then
				if [[ "$x" = "libkadm5clnt.so" ]]; then
				    echo "$x is not a dynamically linked or not stripped, do not separate symbol"
					continue
				elif [[ "$x" = "libkadm5srv.so" ]]; then
					echo "$x is not a dynamically linked or not stripped, do not separate symbol"
				    continue
				fi

				objcopy --only-keep-debug "$x" "$INSTALL_DIR/${x}.symbol" > /dev/null 2>&1
				objcopy --strip-debug "$x" "$x"_release
				rm  "$x"
				mv "$x"_release "$x"
				objcopy --add-gnu-debuglink="$INSTALL_DIR/${x}.symbol" "$x"
				chmod 755 "$INSTALL_DIR/${x}.symbol"
				mv $INSTALL_DIR/${x}.symbol $CPTODEST
			elif [[ "$x" = *".a" ]]; then
				objcopy --only-keep-debug "$x" "$INSTALL_DIR/${x}.symbol" > /dev/null 2>&1
				objcopy --strip-debug "$x" "$x"_release
				rm  "$x"
				mv "$x"_release "$x"
				objcopy --add-gnu-debuglink="$INSTALL_DIR/${x}.symbol" "$x"
				chmod 755 "$INSTALL_DIR/${x}.symbol"
				mv $INSTALL_DIR/${x}.symbol $CPTODEST
			fi
		elif [ -d "$x" ];then
			separate_symbol "$x"
		fi

	done
	INSTALL_DIR=$INSTALL_DIR/..
	CPTODEST=$CPTODEST/..
	cd ..
}
while [ $# -gt 0 ]; do   
    case "$1" in   
  
        -p)
		if [ "$2"X = X ];then
			echo "no given generration install path"
			exit 1
		fi
		INSTALL_DIR=$2  
		echo $INSTALL_DIR
		shift 2  
		;;  
	-h|--help)                                                                                                                                
		echo "Usage: $0 [OPTION]
		-h|--help  	show help information
		-p		provode the install path of software"
		exit 1
		;;
    esac   

done 
SYMBOLS_DIR=$INSTALL_DIR/symbols
PACKAGE_DIR=$INSTALL_DIR/packages
mkdir $SYMBOLS_DIR
mkdir $PACKAGE_DIR

create_dir bin
create_dir lib
CPTODEST=$SYMBOLS_DIR
cd 	$INSTALL_DIR
separate_symbol "$BIN_DIR"
cd 	$INSTALL_DIR
separate_symbol "$LIB_DIR"
cd $SYMBOLS_DIR/../
tar -zcf $PACKAGE_DIR/symbols.tar.gz symbols
chmod 755 $PACKAGE_DIR/symbols.tar.gz
cp $PACKAGE_DIR/symbols.tar.gz  $DEPTH
