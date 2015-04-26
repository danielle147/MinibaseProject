JDKPATH = /p/java-1.5
LIBPATH = ../../lib/bufmgr.jar:../../lib/diskmgr.jar:../../lib/heap.jar:../../lib/index.jar
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH):../relop:../global
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

xx:
	jar tf $(LIBPATH)

clean:
	\rm -f *.class *~ \#* core TRACE


