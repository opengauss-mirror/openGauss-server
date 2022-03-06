ifdef BUILD_FOR_ARM
CXX=aarch64-linux-gnu-gcc
else
CXX=gcc
endif
EXTRA_LIB_DIR=-L../lib
EXTRA_INCLUDE_DIR=-I../include

LIB=-leSDKOBS -l$(SECURE_C_CHECK)
LINKOPT=$(EXTRA_LIB_DIR) $(LIB)

TARGET_TOOL=obstool

SRC_TOOL=obstool.c

export LD_LIBRARY_PATH=../lib
all: obstool
$(TARGET_TOOL):
	$(CXX) $(SRC_TOOL) $(EXTRA_INCLUDE_DIR) $(LINKOPT) -lpthread -fstack-protector --param ssp-buffer-size=4 -Wstack-protector -Wl,--disable-new-dtags,--rpath ../lib -Wl,-z,relro,-z,now -Wl,-z,noexecstack -fPIC -o $(TARGET_TOOL)
clean:
	rm -f obstool;rm -f *.o
