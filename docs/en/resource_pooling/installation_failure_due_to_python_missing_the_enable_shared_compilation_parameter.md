# Installation Failure Due to Python Missing the --enable-shared Compilation Parameter

## Symptom

During the pre-installation of the openGauss Enterprise Edition, the following error message appears:

```
Exception: [GAUSS-52200]: When compiling python, carry the -enable-shared parameters
```

## Troubleshooting Method

Check whether the compilation option exists by using Python's `sysconfig.get_config_var` method.

## Root Cause

Inspecting the compilation options using Python's `sysconfig.get_config_var` method reveals that the `--enable-shared` flag is missing from the build configuration.

```shell
Python 3.7.9 (default, Dec 31 2021, 20:01:11)
[GCC 7.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import sysconfig
>>> sysconfig.get_config_var("CONFIG_ARGS")
"'--build=aarch64-openEuler-linux-gnu' '--host=aarch64-openEuler-linux-gnu' '--program-prefix=' '--disable-dependency-tracking' '--prefix=/usr' '--exec-prefix=/usr' '--bindir=/usr/bin' '--sbindir=/usr/sbin' '--sysconfdir=/etc' '--datadir=/usr/share' '--includedir=/usr/include' '--libdir=/usr/lib64' '--libexecdir=/usr/libexec' '--localstatedir=/var' '--sharedstatedir=/var/lib' '--mandir=/usr/share/man' '--infodir=/usr/share/info' '--enable-ipv6' '--with-computed-gotos=yes' '--with-dbmliborder=gdbm:ndbm:bdb' '--with-system-expat' '--with-system-ffi' '--enable-loadable-sqlite-extensions' '--with-dtrace' '--with-ssl-default-suites=openssl' '--with-valgrind' '--without-ensurepip' '--disable-optimizations' 'build_alias=aarch64-openEuler-linux-gnu' 'host_alias=aarch64-openEuler-linux-gnu' 'CFLAGS= -D_GNU_SOURCE -fPIC -fwrapv ' 'LDFLAGS= -g ' 'CPPFLAGS=' 'PKG_CONFIG_PATH=:/usr/lib64/pkgconfig:/usr/share/pkgconfig'"
```

## Solution

Add the `--enable-shared` option when compiling Python, then run the compilation command again.
