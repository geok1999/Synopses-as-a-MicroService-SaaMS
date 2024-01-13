@echo off
set KAFKA_HOME=C:\kafka
cd /d %KAFKA_HOME%
bin\windows\kafka-server-start.bat config\server.properties