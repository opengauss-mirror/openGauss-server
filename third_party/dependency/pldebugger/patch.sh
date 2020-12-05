rm -rf pldebugger_3_0
tar xfzv pldebugger_3_0.tar.gz &> /dev/null
rename ".c" ".cpp" pldebugger_3_0/*.c
file_name="pldebugger_3_0_patch.patch"
if [ ! -f "$file_name" ]; then 
	echo "ERROR: $file_name does not exist."
	exit 1; 
fi
patch -p0 -d pldebugger_3_0 < $file_name
file_name="pldebugger_3_0_patch2.patch"
if [ ! -f "$file_name" ]; then
	echo "ERROR: $file_name does not exist."
	exit 1;
fi
patch -p0 -d pldebugger_3_0 < $file_name
