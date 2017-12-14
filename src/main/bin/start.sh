#!/bin/bash

script_path=$(cd "$(dirname "$0")"; pwd)
lib_path=${script_path}/../lib
main_class="com.scistor.process.thrift.server.StartSlaveServer"
conf_path=${script_path}/../conf

CLASSPATH="${conf_path}"
for jar in $lib_path/*.jar
do
	if [ "$CLASSPATH" = "" ] ; then
		CLASSPATH=$jar
	else
		CLASSPATH=$CLASSPATH:$jar
	fi
done

export CLASSPATH=$CLASSPATH
chmod a+x ${script_path}/../jdk1.7.0_80/bin/java
${script_path}/../jdk1.7.0_80/bin/java -jar ${script_path}/../SlaveServer-1.0-SNAPSHOT.jar
