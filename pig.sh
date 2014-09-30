#!/bin/sh

sudo -u hdfs hadoop fs -rm -R /user/oozie/share/lib
sudo oozie-setup sharelib create -fs hdfs://localhost -locallib /usr/lib/oozie/oozie-sharelib-yarn.tar.gz
sudo service oozie stop
sudo service oozie start
