import os

# Windows platform does not have this method
if getattr(os, 'getuid', None) is None:
    os.getuid = lambda: 1

if getattr(os, 'sysconf', None) is None:
    os.sysconf = lambda x: x

if getattr(os, 'mknod', None) is None:
    os.mknod = lambda x: x