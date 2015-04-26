JDKPATH = /usr
LIBPATH = ../../lib/bufmgr.jar:../../lib/diskmgr.jar:../../lib/heap.jar:../../lib/index.jar
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH):../relop:../global
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = rotest

all: $(PROGS)

# test relop

ROTest:ROTest.java
	$(JAVAC) ../global/*.java
	$(JAVAC) ../relop/*.java
	$(JAVAC) TestDriver.java ROTest.java
	
rotest: ROTest
	$(JAVA) tests.ROTest

xx:
	jar tf $(LIBPATH)

clean:
	\rm -f *.class *~ \#* core TRACE
