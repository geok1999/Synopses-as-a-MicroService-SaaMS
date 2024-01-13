@echo off

:: Your existing logic here to stop servers and clean directories

:: ...

:: Start Kafka and Zookeeper servers

:: ...

:: Start consuming messages from OutputTopicSynopsis1 topic
start /min cmd.exe /k "cd /d C:\kafka && bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic OutputTopicSynopsis1 --from-beginning"
echo Kafka Consumer started for topic OutputTopicSynopsis1...

:: Start consuming messages from OutputTopicSynopsis2 topic
start /min cmd.exe /k "cd /d C:\kafka && bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic OutputTopicSynopsis2 --from-beginning"
echo Kafka Consumer started for topic OutputTopicSynopsis2...

:: Start consuming messages from OutputTopicSynopsis3 topic
start /min cmd.exe /k "cd /d C:\kafka && bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic OutputTopicSynopsis4 --from-beginning"
echo Kafka Consumer started for topic OutputTopicSynopsis3...
