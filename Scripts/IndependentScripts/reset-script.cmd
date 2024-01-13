@echo off
REM Stop Kafka server
CALL C:\kafka\bin\windows\kafka-server-stop.bat

REM Stop Zookeeper server
CALL C:\kafka\bin\windows\zookeeper-server-stop.bat

REM Remove the Kafka and Zookeeper data directories
RD /S /Q C:\kafka\kafka-logs
RD /S /Q C:\kafka\zookeeper-data
IF EXIST C:\dataset\topicCounter\topicCounter.txt del C:\dataset\topicCounter\topicCounter.txt && echo topicCounter.txt file deleted...