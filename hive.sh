#!/bin/sh

grep hive.aux.jars.path /usr/lib/hive/conf/hive-site.xml >/dev/null 2>&1

if [ $?=1 ]; then
  sed -e '\#</configuration># i\
  <property>\
    <name>hive.aux.jars.path</name>\
    <value>file:////usr/lib/hive/lib/hive-contrib.jar</value>\
    <description>Add the Hive contrib JAR to Hive</description>\
  </property>'
fi
