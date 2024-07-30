JDKPATH = /usr
LIBPATH = /p/course/cs784-raghu/minibase_share/javaminibase/construction
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

hf:Columnarfile.java
	$(JAVAC) Columnarfile.java

hp:CFException.java
	$(JAVAC) CFException.java

tuple:TupleScan.java
	$(JAVAC) TupleScan.java

scan:ValueClassIntf.java
	$(JAVAC) ValueClassIntf.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
