subdir = distribute/test/regress/jdbc_client
top_builddir = ../../../../opengauss
include $(top_builddir)/src/Makefile.global

JFLAGS = -cp src:../jdbc_client_lib/gsjdbc4.jar -d classes
JRFLAGS = cfm jdbc_client.jar manifest.txt -C classes gauss

JC = ${JAVA_HOME}/bin/javac
JR = ${JAVA_HOME}/bin/jar


.SUFFIXES: .java .class

JCLASSES = $(shell find $(.) -name '*.java')

all: jdbc_client.jar

jdbc_client.jar: cclasses
	$(JR) $(JRFLAGS)

classes:
	mkdir -p classes

.java.class: classes 
	$(JC) $(JFLAGS) $*.java

cclasses: classes $(JCLASSES:.java=.class)

clean:
	rm -rf classes
	rm -f jdbc_client.jar
