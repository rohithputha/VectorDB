# JDKPATH = /usr
# LIBPATH = .:..
# JDKPATH = /usr/lib/jvm/java-8-openjdk-amd64
JDKPATH = /usr/lib/jvm/java-23-amazon-corretto
LIBPATH = /home/sandipan/Documents/ASU/S25-CSE510/VectorDB/src
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core
