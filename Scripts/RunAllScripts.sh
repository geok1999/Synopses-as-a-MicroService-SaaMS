#!/bin/bash

# Define Kafka installation path
KAFKA_INSTALL_DIR="/kafka"

# Stop Kafka server (Broker 0)
$KAFKA_INSTALL_DIR/bin/kafka-server-stop.sh $KAFKA_INSTALL_DIR/config/server.properties
echo "Kafka server 0 stopped..."

# Stop Kafka server (Broker 1)
$KAFKA_INSTALL_DIR/bin/kafka-server-stop.sh $KAFKA_INSTALL_DIR/config/server-1.properties
echo "Kafka server 1 stopped..."

# Stop Kafka server (Broker 2)
$KAFKA_INSTALL_DIR/bin/kafka-server-stop.sh $KAFKA_INSTALL_DIR/config/server-2.properties
echo "Kafka server 2 stopped..."

# Stop Zookeeper server
$KAFKA_INSTALL_DIR/bin/zookeeper-server-stop.sh
echo "Zookeeper server stopped..."

# Remove the Kafka Streams state directory
rm -rf /dataset/tmp/kafka-streams/
echo "Kafka Streams state directory removed..."

# Remove the Kafka and Zookeeper data directories
rm -rf /kafka/kafka-logs
rm -rf /kafka/kafka-logs-1
rm -rf /kafka/kafka-logs-2
rm -rf /kafka/zookeeper-data
echo "Data directories removed..."

# Start Zookeeper server in a new terminal
nohup $KAFKA_INSTALL_DIR/bin/zookeeper-server-start.sh $KAFKA_INSTALL_DIR/config/zookeeper.properties > /dev/null 2>&1 &
echo "Zookeeper server started..."

# Start Kafka servers in new terminals
nohup $KAFKA_INSTALL_DIR/bin/kafka-server-start.sh $KAFKA_INSTALL_DIR/config/server.properties > /dev/null 2>&1 &
echo "Kafka server 0 started..."

nohup $KAFKA_INSTALL_DIR/bin/kafka-server-start.sh $KAFKA_INSTALL_DIR/config/server-1.properties > /dev/null 2>&1 &
echo "Kafka server 1 started..."

nohup $KAFKA_INSTALL_DIR/bin/kafka-server-start.sh $KAFKA_INSTALL_DIR/config/server-2.properties > /dev/null 2>&1 &
echo "Kafka server 2 started..."