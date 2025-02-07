tar -zxf source.tar.gz
cp src_mock.cpp source/src_mock.cpp
cp Makefile source/Makefile
cd source
make build_shared -sj
cp libog_query.so ../libog_query.so