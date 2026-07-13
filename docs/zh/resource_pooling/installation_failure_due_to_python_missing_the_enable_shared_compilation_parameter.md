# 因python缺少--enable-shared编译参数导致安装失败的问题

## 一、问题现象

openGauss企业版预安装过程中，出现以下报错信息。

```
Exception: [GAUSS-52200]: When compiling python, carry the -enable-shared parameters
```

## 二、定位方法

通过python的`sysconfig.get_config_var`方法检查编译选项是否存在。

## 三、问题根因

通过python的`sysconfig.get_config_var`方法检查编译选项，缺少`--enable-shared`项。

```shell
Python 3.7.9 (default, Dec 31 2021, 20:01:11)
[GCC 7.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import sysconfig
>>> sysconfig.get_config_var("CONFIG_ARGS")
"'--build=aarch64-openEuler-linux-gnu' '--host=aarch64-openEuler-linux-gnu' '--program-prefix=' '--disable-dependency-tracking' '--prefix=/usr' '--exec-prefix=/usr' '--bindir=/usr/bin' '--sbindir=/usr/sbin' '--sysconfdir=/etc' '--datadir=/usr/share' '--includedir=/usr/include' '--libdir=/usr/lib64' '--libexecdir=/usr/libexec' '--localstatedir=/var' '--sharedstatedir=/var/lib' '--mandir=/usr/share/man' '--infodir=/usr/share/info' '--enable-ipv6' '--with-computed-gotos=yes' '--with-dbmliborder=gdbm:ndbm:bdb' '--with-system-expat' '--with-system-ffi' '--enable-loadable-sqlite-extensions' '--with-dtrace' '--with-ssl-default-suites=openssl' '--with-valgrind' '--without-ensurepip' '--disable-optimizations' 'build_alias=aarch64-openEuler-linux-gnu' 'host_alias=aarch64-openEuler-linux-gnu' 'CFLAGS= -D_GNU_SOURCE -fPIC -fwrapv ' 'LDFLAGS= -g ' 'CPPFLAGS=' 'PKG_CONFIG_PATH=:/usr/lib64/pkgconfig:/usr/share/pkgconfig'"
```

## 四、解决方法

编译python时添加`--enable-shared`选项，然后再次执行编译命令。
