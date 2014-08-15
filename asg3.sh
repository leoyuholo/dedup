#!/bin/bash

prog="MyDedup"
des="/home/hadoop/asg3"
for_keystone="/home/hadoop/deduplication/java-cloudfiles-for_keystone2.0"
export CLASSPATH="${for_keystone}/lib/httpcore-4.1.4.jar:${for_keystone}/lib/commons-cli-1.1.jar:${for_keystone}/lib/httpclient-4.1.3.jar:${for_keystone}/lib/commons-lang-2.4.jar:${for_keystone}/lib/junit.jar:${for_keystone}/lib/commons-codec-1.3.jar:${for_keystone}/lib/commons-io-1.4.jar:${for_keystone}/lib/commons-logging-1.1.1.jar:${for_keystone}/lib/log4j-1.2.15.jar:dist/java-cloudfiles.jar:${for_keystone}/lib/org-json.jar:."

export CLOUDFILES_USERNAME="csci4180-10:csci4180-10"
export CLOUDFILES_PASSWORD="20120927"

if [ "$#" = "0" ]; then
	echo "Usage $0 compile|run"
	exit 0
else
	if [ "$1" = "compile" ]; then
		test -f "${prog}.class" && rm ./*.class
		javac -classpath "${CLASSPATH}" "${des}/${prog}.java" -Xlint
		# javac -classpath "${CLASSPATH}" "${des}/${prog}.java"
		test -f "${prog}.class" && echo -e "Compile Done!"
		test ! -f "${prog}.class" && echo -e "Failed"
	elif [ "$1" = "run" ]; then
		# java "${prog}" $@ 
		java "${prog}"
	fi
fi

