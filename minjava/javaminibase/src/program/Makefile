# modify the paths appropriately
JDKPATH = /usr
# JDKPATH = /usr
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# build query

Query:Query.java
	$(JAVAC) *.java

clean:
	rm -f *.class
