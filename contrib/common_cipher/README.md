动态库使用说明：
a.硬件动态库中目前适配的三种硬件信息如下：
三未信安密钥管理系统，型号SYT1306
江南天安密钥管理系统，型号SJJ1988
光电安辰PCI-E密码卡，型号TOEC-GMPCIE01

b.硬件动态库使用需要配置的入参
指定所使用的硬件,必须项，取值范围["GDACCARD" "JNTAKMS" "SWXAKMS"]。
指定具体硬件提供so所在路径，必须项。
硬件配置文件所在路径：密钥管理系统需要配置此变量，指定kms的配置文件路径，可选项。
江南天安配置文件只需要传入路径，三未信安需要带配置文件名称。
配置示例:
MODULE_TYPE=GDACCARD,MODULE_LIB_PATH=/home/lib/libsdf.so
MODULE_TYPE=JNTAKMS,MODULE_LIB_PATH=/home/lib/libsdf.so,MODULE_CONFIG_FILE_PATH=/home/etc/
MODULE_TYPE=SWXAKMS,MODULE_LIB_PATH=/home/lib/libsdf.so,MODULE_CONFIG_FILE_PATH=/home/etc/xxx.ini

使用具体的接口详见：common_cipher.h

