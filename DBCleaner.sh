#!/bin/bash
script_name=$0

echo "Clearing Influxdb instance..."
sudo service influxdb stop
sudo dpkg --purge influxdb
sudo find / -name "influx*" -not -path "/opt/grafana/*" -exec rm -r "{}" \;
echo "Clearing completed" 

echo "Downloading Influxdb package..."
wget https://s3.amazonaws.com/influxdb/influxdb_0.9.1_amd64.deb
echo "Package downloaded"

echo "Installing package"
sudo dpkg -i influxdb_0.9.1_amd64.deb

echo "Generating config  /etc/opt/influxdb/influxdb.conf"
sudo /opt/influxdb/influxd config > $HOME/influxdb.conf
sudo sed  -i 's/\/root\/.influxdb/\/var/\opt/\influxdb/g' $HOME/influxdb.conf

sudo mv $HOME/influxdb.conf /etc/opt/influxdb/influxdb.conf
sudo rm $HOME/influxdb.conf



echo "Starting daemon...."
sudo /etc/init.d/influxdb start
sudo service influxdb status


sudo ln -s $HOME/DBCleaner.sh /usr/bin/influxdbcleaner
echo "Symbolic link /usr/bin/influxdbcleaner created"
echo "<$script_name> task completed" 
exit 0
