#!/bin/bash

mode="B"
for i in "$@"
do
case $i in
    --mode=*)
    mode="${i#*=}"
    shift
    ;;
    *)
    ;;
esac
done

if [ "$mode" != "A" ] && [ "$mode" != "B" ]; then
  echo "Error: Invalid value for --mode. Allowed values are 'A' or 'B'."
  exit 1
fi

if [ "$mode" == "A" ]; then
    mkdir -p source_server
    tar -zxf source_server.tar.gz -C source_server
    cp src_mock.cpp source_server/source/src_mock.cpp
    cp Makefile source_server/source/Makefile
    cd source_server/source
    make build_shared -sj
elif [ "$mode" == "B" ]; then
    mkdir -p source_dolphin
    tar -zxf source_dolphin.tar.gz -C source_dolphin
    cp src_mock.cpp source_dolphin/source/src_mock.cpp
    cp Makefile source_dolphin/source/Makefile
    cd source_dolphin/source
    make CFLAGS="-DDOLPHIN" CXXFLAGS="-DDOLPHIN" build_shared -sj
fi

cp libog_query.so ../../libog_query.so
