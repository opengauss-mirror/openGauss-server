#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2020-2025. All rights reserved.
# descript: Separate debug information

DEPTH=$(pwd)
INSTALL_DIR=$DEPTH/../../mppdb_temp_install

# get os name
KERNEL=""
if [ -f "/etc/euleros-release" ]; then
	KERNEL=$(cat /etc/euleros-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
else
	KERNEL=$(lsb_release -a | grep "Description" | awk -F ' ' '{print $2}'| tr A-Z a-z)
fi

if [ "$KERNEL"x = "red"x ]; then
	platformname="Redhat"	
elif [ "$KERNEL"x = "euleros"x ]; then
	platformname="Euler"
else
	platformname="Suse"	
fi

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
	local INSTALL_DIR=$INSTALL_DIR/$1
	cd $1
	local CPTODEST=$CPTODEST/$1
	for x in $(ls)
	do

		if [ -L "$x" ];then
			echo "$x is a link, do not separate symbol"
		elif [[ "$x" = *".py" ]];then
			echo "$x is a script, do not separate symbol"
		elif [[ "$x" = *".dat" ]];then
			echo "$x is a license file, do not separate symbol"
                # The following second condition judges whether the file is a shell script without a suffix name. 
                # Usually, executable shell script has a header comment that indicates which interpreter to use, 
                # e.g., "#!/usr/bin/env bash".
		elif [[ "$x" = *".sh" ]] || [[ -f "$x" && -x "$x" && "$(head -c2 $x)" == '#!' ]]; then
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
  			symbol_name=$x
			symbol_num=`echo ${#symbol_name}`
  			let symbol_add=${symbol_num}%4
  			case ${symbol_add} in
  			0)
  			symbol_name=${symbol_name}
  			;;
  			1)
  			symbol_name=${symbol_name}000
  			;;
  			2)
  			symbol_name=${symbol_name}00
  			;;
  			3)
  			symbol_name=${symbol_name}0
  			;;
  			esac
			if [ -x "$x" ]; then
				if [ "$x" != "install-sh"  ]; then
					objcopy --only-keep-debug "$x" "$INSTALL_DIR/${symbol_name}.symbol" > /dev/null 2>&1
					objcopy --strip-all "$x" "$x"_release
					rm  "$x"
					mv "$x"_release "$x"
					objcopy --add-gnu-debuglink="$INSTALL_DIR/${symbol_name}.symbol" "$x"
					chmod 755 "$INSTALL_DIR/${symbol_name}.symbol"
					mv $INSTALL_DIR/${symbol_name}.symbol $CPTODEST
				fi
			elif [[ "$x" = *".so" || "$x" = *".so."* ]]; then
			        if [[ "$platformname" = "Redhat" ]] || [[ "$platformname" = "Euler" ]]; then
				    if [[ "$x" = "libkadm5clnt.so" ]]; then
				    	    echo "$x is not a dynamically linked or not stripped, do not separate symbol"
					    continue
				    elif [[ "$x" = "libkadm5srv.so" ]]; then
					    echo "$x is not a dynamically linked or not stripped, do not separate symbol"
				            continue
				    fi
				fi			
				objcopy --only-keep-debug "$x" "$INSTALL_DIR/${symbol_name}.symbol" > /dev/null 2>&1
				objcopy --strip-all "$x" "$x"_release
				rm  "$x"
				mv "$x"_release "$x"
				objcopy --add-gnu-debuglink="$INSTALL_DIR/${symbol_name}.symbol" "$x"
				chmod 755 "$INSTALL_DIR/${symbol_name}.symbol"
				mv $INSTALL_DIR/${symbol_name}.symbol $CPTODEST
			elif [[ "$x" = *".a" ]]; then
				objcopy --only-keep-debug "$x" "$INSTALL_DIR/${symbol_name}.symbol" > /dev/null 2>&1
				objcopy --strip-debug "$x" "$x"_release
				rm  "$x"
				mv "$x"_release "$x"
				objcopy --add-gnu-debuglink="$INSTALL_DIR/${symbol_name}.symbol" "$x"
				chmod 755 "$INSTALL_DIR/${symbol_name}.symbol"
				mv $INSTALL_DIR/${symbol_name}.symbol $CPTODEST
			elif [[ "$x" =~ [0-9]$ ]]; then
				objcopy --only-keep-debug "$x" "$INSTALL_DIR/${symbol_name}.symbol" > /dev/null 2>&1
				objcopy --strip-all "$x" "$x"_release
				rm  "$x"
				mv "$x"_release "$x"
				objcopy --add-gnu-debuglink="$INSTALL_DIR/${symbol_name}.symbol" "$x"
				chmod 755 "$INSTALL_DIR/${symbol_name}.symbol"
				mv $INSTALL_DIR/${symbol_name}.symbol $CPTODEST
			fi
		elif [ -d "$x" ];then
			separate_symbol "$x"
		fi

	done
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
