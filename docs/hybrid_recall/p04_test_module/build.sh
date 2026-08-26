#!/usr/bin/env bash
# P0-4 测试模块构建：复用 executor 编译参数（相对 -I 路径须在 executor 目录执行）
set -euo pipefail
EXEC_DIR=/home/qiumc/data/code/cpp/openGauss-server/src/gausskernel/runtime/executor
MOD=/home/qiumc/data/code/cpp/openGauss-server/docs/hybrid_recall/p04_test_module
GCC_PATH=/home/qiumc/data/code/cpp/binarylibs/buildtools/gcc10.3
export PATH="$GCC_PATH/gcc/bin:$PATH"
export LD_LIBRARY_PATH="$GCC_PATH/gcc/lib64:$GCC_PATH/isl/lib:$GCC_PATH/mpc/lib:$GCC_PATH/mpfr/lib:$GCC_PATH/gmp/lib:${LD_LIBRARY_PATH:-}"
cd "$EXEC_DIR"
touch nodeTidscan.cpp
CMD=$(make -n nodeTidscan.o 2>/dev/null | grep "nodeTidscan" | head -1 | sed 's/ -c / /g; s/nodeTidscan\.cpp//g; s/ -o nodeTidscan\.o//g')
[[ -n "$CMD" ]] || { echo "捕获编译命令失败"; exit 1; }
eval "$CMD -fPIC -shared $MOD/hybrid_recall_test.cpp -o $MOD/hybrid_recall_test.so"
nm -D "$MOD/hybrid_recall_test.so" | grep -q " T hybrid_recall_debug_execute$" || { echo "符号未导出为 C 链接"; exit 1; }
nm -D "$MOD/hybrid_recall_test.so" | grep -q " T hybrid_recall_debug_rescan$" || { echo "rescan 符号未导出"; exit 1; }
rm -f /home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install/lib/postgresql/proc_srclib/hybrid_recall_test.so
cp "$MOD/hybrid_recall_test.so" /home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install/lib/postgresql/proc_srclib/
rm -f /home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install/lib/postgresql/proc_srclib/hybrid_recall_p35_test.so
cp "$MOD/hybrid_recall_test.so" /home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install/lib/postgresql/proc_srclib/hybrid_recall_p35_test.so
echo "构建并部署完成"
