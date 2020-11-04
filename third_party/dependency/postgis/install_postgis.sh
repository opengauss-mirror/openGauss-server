#!/bin/bash
set -e
export toolhome=$GAUSSHOME/pggis_tools
export PGGIS_DIR=$CODE_BASE/third_party/dependency/postgis
platform=`sh $CODE_BASE/src/get_PlatForm_str.sh`

#main function of install tools
function install_tools()
{
echo '-----install geos'
if [ ! -d "$toolhome/geos" ]; then
    echo "the geos will be installed under $toolhome/geos"
    install_geos
else
  read -p "The geos is already installed, remove it and re-install?: [y/n] " answer
  if [ "$answer" == "y" ]; then
    install_geos
  else
    echo "skip to reinstalled geos"
  fi
fi

echo '-----install proj'
if [ ! -d "$toolhome/proj" ]; then
    echo "the proj will be installed under $toolhome/proj"
    install_proj
else
  read -p "The proj is already installed, remove it and re-install?: [y/n] " answer
  if [ "$answer" == "y" ]; then
    install_proj
  else
    echo "skip to reinstalled proj"
  fi
fi

#install JSON-C
echo '-----install JSON-C'
if [ ! -d "$toolhome/json" ]; then
    echo "the json will be installed under $toolhome/json"
    install_json
else
  read -p "The json is already installed, remove it and re-install?: [y/n] " answer
  if [ "$answer" == "y" ]; then
    install_json
  else
    echo "skip to reinstalled json"
  fi
fi

#install Libxml2
echo '-----install Libxml2'
if [ ! -d "$toolhome/libxml2" ]; then
    echo "the libxml2 will be installed under $toolhome/libxml2"
    install_libxml2
else
  read -p "The libxml2 is already installed, remove it and re-install?: [y/n] " answer
  if [ "$answer" == "y" ]; then
    install_libxml2
  else
    echo "skip to reinstalled libxml2"
  fi
fi

#install Gdal
echo '-----install Gdal'
if [ ! -d "$toolhome/gdal" ]; then
    echo "the gdal will be installed under $toolhome/gdal"
    install_gdal
else
  read -p "The gdal is already installed, remove it and re-install?: [y/n] " answer
  if [ "$answer" == "y" ]; then
    install_gdal
  else
    echo "skip to reinstalled gdal"
  fi
fi
}

function install_proj()
{
    cd $PGGIS_DIR/postgis-xc/proj-4.9.2
    if [ "$platform" == "openeuler_aarch64" ]; then
        ./configure --build=aarch64-unknown-linux-gnu --prefix=$toolhome/proj -q
    else
        ./configure --prefix=$toolhome/proj -q
    fi
    make -sj && make install -sj
}

function install_libxml2()
{
    cd $PGGIS_DIR/postgis-xc/libxml2-2.7.1
    chmod +x ./configure
    if [ "$platform" == "openeuler_aarch64" ]; then
        ./configure --build=aarch64-unknown-linux-gnu --prefix=$toolhome/libxml2 -q
    else
        ./configure --prefix=$toolhome/libxml2 -q
    fi
    make -sj && make install -sj
}

function install_json()
{
    cd $PGGIS_DIR/postgis-xc/json-c-json-c-0.12.1-20160607
    chmod +x ./configure
    if [ "$platform" == "openeuler_aarch64" ]; then
        ./configure --build=aarch64-unknown-linux-gnu --prefix=$toolhome/json -q
    else
        ./configure --prefix=$toolhome/json -q
    fi
    make -sj && make install -sj
}

function install_gdal()
{
    cd $PGGIS_DIR/postgis-xc/gdal-1.11.0
    chmod +x ./configure
    chmod +x ./install-sh
    if [ "$platform" == "openeuler_aarch64" ]; then
        ./configure --build=aarch64-unknown-linux-gnu --prefix=$toolhome/gdal --with-xml2=$toolhome/libxml2/bin/xml2-config --with-geos=$toolhome/geos/bin/geos-config --with-static-proj4=$toolhome/proj CFLAGS='-O2 -fpermissive -pthread' -q
    else
        ./configure --prefix=$toolhome/gdal --with-xml2=$toolhome/libxml2/bin/xml2-config --with-geos=$toolhome/geos/bin/geos-config --with-static-proj4=$toolhome/proj CFLAGS='-O2 -fpermissive -pthread' -q
    fi
    make -sj || make -sj
    make install -sj
}

function install_geos()
{
    cd $PGGIS_DIR/postgis-xc/geos-3.6.2
    chmod +x ./configure
    if [ "$platform" == "openeuler_aarch64" ]; then
        ./configure --build=aarch64-unknown-linux-gnu --prefix=$toolhome/geos -q
    else
        ./configure --prefix=$toolhome/geos -q
    fi
    make -sj && make install -sj
}

if [ ! -x $GAUSSHOME -o ! -x $CODE_BASE -o ! -x $BINARYLIBS ];then
    echo "PATH 'GAUSSHOME' or 'CODE_BASE' or 'BINARYLIBS' does not exist, please check your env variables."
    exit 1
fi

#get source code
if [ ! -d "$PGGIS_DIR/postgis-xc" ]; then
    wget -P $PGGIS_DIR https://opengauss.obs.cn-south-1.myhuaweicloud.com/dependency/postgis-xc-master-2020-09-17.tar.gz
    if [ ! -x "$PGGIS_DIR/postgis-xc-master-2020-09-17.tar.gz" ]; then
        echo "no executable postgis-xc-master-2020-09-17.tar.gz file under $PGGIS_DIR"
        exit 1
    fi
    mkdir $PGGIS_DIR/postgis-xc && tar -xzvf $PGGIS_DIR/postgis-xc-master-2020-09-17.tar.gz -C $PGGIS_DIR/postgis-xc --strip-components 1 > /dev/null
fi

#install dependent tools
echo "The dependent tools of postgis will be installed under $toolhome"
install_tools

#install postgis
echo '-----install PostGIS'
cd $CODE_BASE
! patch -p1 < $PGGIS_DIR/postgis.patch
cd $PGGIS_DIR/postgis-xc/postgis-2.4.2
chmod +x ./configure
if [ "$platform" == "openeuler_aarch64" ]; then
    ./configure --build=aarch64-unknown-linux-gnu --prefix=$toolhome/pggis2.4.2 --with-pgconfig=$GAUSSHOME/bin/pg_config --with-projdir=$toolhome/proj --with-geosconfig=$toolhome/geos/bin/geos-config --with-jsondir=$toolhome/json --with-xml2config=$toolhome/libxml2/bin/xml2-config --with-raster --with-gdalconfig=$toolhome/gdal/bin/gdal-config --with-topology --without-address-standardizer CFLAGS="-fPIC -O2 -fpermissive -DPGXC -pthread -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -DMEMORY_CONTEXT_CHECKING -w -I$CODE_BASE/contrib/postgis/ -I$BINARYLIBS/dependency/$platform/cjson/comm/include/ -I$BINARYLIBS/dependency/$platform/openssl/comm/include/ -I$BINARYLIBS/dependency/$platform/kerberos/comm/include/ -I$BINARYLIBS/dependency/$platform/libobs/comm/include/" CPPFLAGS="-I$CODE_BASE/contrib/postgis/ -I$BINARYLIBS/dependency/$platform/cjson/comm/include -I$BINARYLIBS/dependency/$platform/libobs/comm/include/ -fpermissive -w -DMEMORY_CONTEXT_CHECKING -D__STDC_FORMAT_MACROS" CC=g++ -q
else
    ./configure --prefix=$toolhome/pggis2.4.2 --with-pgconfig=$GAUSSHOME/bin/pg_config --with-projdir=$toolhome/proj --with-geosconfig=$toolhome/geos/bin/geos-config --with-jsondir=$toolhome/json --with-xml2config=$toolhome/libxml2/bin/xml2-config --with-raster --with-gdalconfig=$toolhome/gdal/bin/gdal-config --with-topology --without-address-standardizer CFLAGS="-fPIC -O2 -fpermissive -DPGXC  -pthread -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -DMEMORY_CONTEXT_CHECKING -w -I$CODE_BASE/contrib/postgis/ -I$BINARYLIBS/dependency/$platform/cjson/comm/include/ -I$BINARYLIBS/dependency/$platform/openssl/comm/include/ -I$BINARYLIBS/dependency/$platform/kerberos/comm/include/ -I$BINARYLIBS/dependency/$platform/libobs/comm/include/" CPPFLAGS="-I$CODE_BASE/contrib/postgis/ -I$BINARYLIBS/dependency/$platform/cjson/comm/include -I$BINARYLIBS/dependency/$platform/libobs/comm/include/ -fpermissive -w -DMEMORY_CONTEXT_CHECKING -D__STDC_FORMAT_MACROS" CC=g++ -q
fi
make -sj && make install -sj
if [ -x "$toolhome/pggis2.4.2/lib/liblwgeom-2.4.so.0" ]; then
    echo "PostGIS installed successfully."
else
    echo "PostGIS installation failed."
    exit 1
fi

#copy the essential files
cp $toolhome/json/lib/libjson-c.so.2 $GAUSSHOME/lib/libjson-c.so.2
cp $toolhome/geos/lib/libgeos_c.so.1 $GAUSSHOME/lib/libgeos_c.so.1
cp $toolhome/proj/lib/libproj.so.9 $GAUSSHOME/lib/libproj.so.9
cp $toolhome/geos/lib/libgeos-3.6.2.so $GAUSSHOME/lib/libgeos-3.6.2.so
cp $toolhome/pggis2.4.2/lib/liblwgeom-2.4.so.0 $GAUSSHOME/lib/liblwgeom-2.4.so.0
cp $PGGIS_DIR/postgis-xc/postgis-2.4.2/postgis.control $GAUSSHOME/share/postgresql/extension/
cp $PGGIS_DIR/postgis-xc/postgis-2.4.2/postgis--2.4.2.sql $GAUSSHOME/share/postgresql/extension/
echo "Ready to restart database and create related extension!"
