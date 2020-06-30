#
# Makefile
#
#
TARGET = entab
BINDIR = /usr/local/bin
XFLAGS =
CFLAGS = -O $(XFLAGS)
LIBS =

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

$(TARGET): entab.o halt.o
	$(CC) -o $@ $(CXXFLAGS) $^ $(LIBS)

clean:
	rm -f *.o $(TARGET) log core *.depend

install: $(TARGET)
	install -s $(TARGET) $(BINDIR)
	rm -f $(BINDIR)/detab
	ln $(BINDIR)/$(TARGET) $(BINDIR)/detab
