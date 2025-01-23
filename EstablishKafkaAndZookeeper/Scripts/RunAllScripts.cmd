@echo off

REM Define Kafka installation directory
SET KAFKA_INSTALL_DIR=C:\kafka

REM Stop Kafka server (Broker 0)
CALL %KAFKA_INSTALL_DIR%\bin\windows\kafka-server-stop.bat %KAFKA_INSTALL_DIR%\config\server.properties
echo Kafka server 0 stopped...

REM Stop Kafka server (Broker 1)
CALL %KAFKA_INSTALL_DIR%\bin\windows\kafka-server-stop.bat %KAFKA_INSTALL_DIR%\config\server-1.properties
echo Kafka server 1 stopped...

REM Stop Kafka server (Broker 2)
CALL %KAFKA_INSTALL_DIR%\bin\windows\kafka-server-stop.bat %KAFKA_INSTALL_DIR%\config\server-2.properties
echo Kafka server 2 stopped...

REM Stop Zookeeper server
CALL %KAFKA_INSTALL_DIR%\bin\windows\zookeeper-server-stop.bat
echo Zookeeper server stopped...

REM Remove the Kafka Streams state directory
RD /S /Q C:\dataset\tmp\kafka-streams\
echo Kafka Streams state directory removed...

REM Remove the Kafka and Zookeeper data directories
RD /S /Q %KAFKA_INSTALL_DIR%\kafka-logs
RD /S /Q %KAFKA_INSTALL_DIR%\kafka-logs-1
RD /S /Q %KAFKA_INSTALL_DIR%\kafka-logs-2
RD /S /Q %KAFKA_INSTALL_DIR%\zookeeper-data
echo Data directories removed...

REM Start Zookeeper server in a new terminal
start /min cmd.exe /C "cd /d %KAFKA_INSTALL_DIR% && bin\windows\zookeeper-server-start.bat config\zookeeper.properties"
echo Zookeeper server started...

REM Start Kafka server in a new terminal
start /min cmd.exe /C "cd /d %KAFKA_INSTALL_DIR% && bin\windows\kafka-server-start.bat config\server.properties"
echo Kafka server 0 started...

REM Start Kafka server in a new terminal
start /min cmd.exe /C "cd /d %KAFKA_INSTALL_DIR% && bin\windows\kafka-server-start.bat config\server-1.properties"
echo Kafka server 1 started...

REM Start Kafka server in a new terminal
start /min cmd.exe /C "cd /d %KAFKA_INSTALL_DIR% && bin\windows\kafka-server-start.bat config\server-2.properties"
echo Kafka server 2 started...
