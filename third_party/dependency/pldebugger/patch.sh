rm -rf pldebugger_3_0
tar xfzv pldebugger_3_0.tar.gz &> /dev/null
rename ".c" ".cpp" pldebugger_3_0/*.c
file_name="huawei_pldebugger.patch"
if [ ! -f "$file_name" ]; then 
	exit 0; 
fi
patch -p0 -d pldebugger_3_0 < $file_name
