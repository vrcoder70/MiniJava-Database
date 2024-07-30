# modify the paths appropriately
JDKPATH = /usr
# JDKPATH = /usr
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

DBDriver:DBDriver.java
	$(JAVAC) DBDriver.java

dbdriver:DBDriver BatchInsert Index Query CmplxQuery
	$(JAVA) driver.DBDriver

BatchInsert:BatchInsert.java
	$(JAVAC) BatchInsert.java

Index: Index.java
	$(JAVAC) Index.java
	
Query:QueryProgram.java
	$(JAVAC) QueryProgram.java

CmplxQuery:ComplexQueryProgram.java
	$(JAVAC) ComplexQueryProgram.java

QueryDelete:QueryDeleteProgram.java
	$(JAVAC) QueryDeleteProgram.java
