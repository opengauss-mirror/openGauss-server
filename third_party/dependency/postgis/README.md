## Introduction

This module contains the  implementation patch and installation scripts of shared libraries "Postgis" extension(version PostGIS-2.4.2), which provides spatial objects for the openGauss database, allowing storage and query of information about location and mapping.



Installation
----------------------------------------------------------------------------

### NOTICE:

The installation of postgis extension is highly dependent on the openGauss-third_party_binarylibs, which you could prepare according to  https://opengauss.org/zh/docs/1.0.1/docs/Compilationguide/%E7%89%88%E6%9C%AC%E7%BC%96%E8%AF%91.html instruction. Please note that if used the provided libs without build, it is possible to get some errors like "*can not find xxxx.la*", in which case you need to copy the corresponding files to that destination.

### Prepare:

Postgis extension is dependent on the following tools (or higher viersion)：

- **GCC-5.4、 zlib、autoconf、automake**

- Geos 3.6.2

- Proj 4.9.2

- Json 0.12.1

- Libxml2 2.7.1
- Gdal 1.11.0

For their installation, you could freely  choose to install them by yourself, and we will only provide the the script  for " *Geos、Proj、Json、Libxml、Gdal 1.11.0* " installation, which you could choose to install by script after " ***GCC-5.4、 zlib、autoconf、automake*** " are installed in the environment.

### Complie and Install by script:

1. Download source code *postgis-xc-master-2020-09-17.tar.gz* from the website:

   https://opengauss.obs.cn-south-1.myhuaweicloud.com/dependency/postgis-xc-master-2020-09-17.tar.gz

2. Move it to **third_party/dependency/postgis/** dictionary.

3. Configure environment variables, add ***__\*** based on the code download location.

   ```
   export CODE_BASE=________     # Path of the openGauss-server file
   export BINARYLIBS=________    # Path of the binarylibs file
   export GAUSSHOME=________    # Path of opengauss installation
   export LD_LIBRARY_PATH=$GAUSSHOME/lib:$GAUSSHOME/pggis_tools/geos/lib:$GAUSSHOME/pggis_tools/proj4/lib:$GAUSSHOME/pggis_tools/gdal/lib:$GAUSSHOME/pggis_tools/libxml2/lib/:$LD_LIBRARY_PATH
   ```

4. Compile and install postgis: 

   ```
   cd $CODE_BASE/contrib/postgis/
   make insatll -sj 
   ```

5. Pldebugger installation finished.

### Complie and Install by yourself:

1. Download source code postgis-xc-master-2020-09-17.tar.gz from the website:

   https://opengauss.obs.cn-south-1.myhuaweicloud.com/dependency/postgis-xc-master-2020-09-17.tar.gz

2. copy it to *$GAUSSHOME/thirdparty/dependency/postgis/ path, unzip the tar.gz* and rename to **postgis-xc**
3. the tools should be installed under the *$GAUSSHOME/pggis_tools* dir :

```
#install geos
cd $GAUSSHOME/thirdparty/dependency/postgis/postgis-xc/geos-3.6.2
chmod +x ./configure
./configure --prefix=$GAUSSHOME/pggis_tools/geos
make -sj
make install -sj

#install proj
cd $GAUSSHOME/postgis-xc/proj-4.9.2
chmod +x ./configure
./configure --prefix=$GAUSSHOME/pggis_tools/proj
make –sj
make install -sj

#install json
cd $GAUSSHOME/postgis-xc/json-c-json-c-0.12.1-20160607
chmod +x ./configure
./configure --prefix=$GAUSSHOME/pggis_tools/json
make -sj
make install -sj

#install libxml2
cd $GAUSSHOME/postgis-xc/libxml2-2.7.1
chmod +x ./configure
./configure --prefix=$GAUSSHOME/pggis_tools/libxml2
make -sj
make install -sj
```

4. install postgis, remember to add "*--build=aarch64-unknown-linux-gnu*" if your system is **openeuler_aarch64**, and complete the "____" with your system type:

```
export toolhome=$GAUSSHOME/pggis_tools
cd $CODE_BASE
patch -p1 < thirdparty/dependency/postgis/postgis.patch
cd $PGGIS_DIR/postgis-xc/postgis-2.4.2

#complile
./configure --prefix=$toolhome/pggis2.4.2 --with-pgconfig=$GAUSSHOME/bin/pg_config --with-projdir=$toolhome/proj --with-geosconfig=$toolhome/geos/bin/geos-config --with-jsondir=$toolhome/json --with-xml2config=$toolhome/libxml2/bin/xml2-config --with-raster --with-gdalconfig=$toolhome/gdal/bin/gdal-config --with-topology --without-address-standardizer CFLAGS="-fPIC -O2 -fpermissive -DPGXC -pthread -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -DMEMORY_CONTEXT_CHECKING -w -I$CODE_BASE/contrib/postgis/ -I$BINARYLIBS/dependency/____/cjson/comm/include/ -I$BINARYLIBS/dependency/____/openssl/comm/include/ -I$BINARYLIBS/dependency/____/kerberos/comm/include/ -I$BINARYLIBS/dependency/____/libobs/comm/include/" CPPFLAGS="-I$CODE_BASE/contrib/postgis/ -I$BINARYLIBS/dependency/____/cjson/comm/include -I$BINARYLIBS/dependency/____/libobs/comm/include/ -fpermissive -w -DMEMORY_CONTEXT_CHECKING -D__STDC_FORMAT_MACROS" CC=g++ -q

#install
make -sj && make install -sj
```

5. copy the essential files to opengauss install folders:

```
cp $toolhome/json/lib/libjson-c.so.2 $GAUSSHOME/lib/libjson-c.so.2
cp $toolhome/geos/lib/libgeos_c.so.1 $GAUSSHOME/lib/libgeos_c.so.1
cp $toolhome/proj/lib/libproj.so.9 $GAUSSHOME/lib/libproj.so.9
cp $toolhome/geos/lib/libgeos-3.6.2.so $GAUSSHOME/lib/libgeos-3.6.2.so
cp $toolhome/pggis2.4.2/lib/liblwgeom-2.4.so.0 $GAUSSHOME/lib/liblwgeom-2.4.so.0
cp $PGGIS_DIR/postgis-xc/postgis-2.4.2/postgis.control $GAUSSHOME/share/postgresql/extension/
cp $PGGIS_DIR/postgis-xc/postgis-2.4.2/postgis--2.4.2.sql $GAUSSHOME/share/postgresql/extension/
```