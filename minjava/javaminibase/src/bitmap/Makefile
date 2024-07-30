JDKPATH = /usr
LIBPATH = /p/course/cs784-raghu/minibase_share/javaminibase/construction
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

bm:BM.java
	$(JAVAC) BM.java

bmf:BitMapFile.java
	$(JAVAC) BitMapFile.java

bmhp:BitMapHeaderPage.java
	$(JAVAC) BitMapHeaderPage.java

bmp:BMPage.java
	$(JAVAC) BMPage.java
	
bmfs:BMFileScan.java
	$(JAVAC) BMFileScan.java
	
cbmf:CBitMapFile.java
	$(JAVAC) CBitMapFile.java

cbmhp:CBitMapHeaderPage.java
	$(JAVAC) CBitMapHeaderPage.java

cbmp:CBMPage.java
	$(JAVAC) CBMPage.java
	
cbmfs:CBMFileScan.java
	$(JAVAC) CBMFileScan.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
