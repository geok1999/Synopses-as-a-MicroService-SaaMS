@echo off

REM Stop Kafka server (Broker 0)
CALL C:\kafka\bin\windows\kafka-server-stop.bat C:\kafka\config\server.properties
echo Kafka server 0 stopped...

REM Stop Kafka server (Broker 1)
CALL C:\kafka\bin\windows\kafka-server-stop.bat C:\kafka\config\server-1.properties
echo Kafka server 1 stopped...

REM Stop Kafka server (Broker 2)
CALL C:\kafka\bin\windows\kafka-server-stop.bat C:\kafka\config\server-2.properties
echo Kafka server 2 stopped...

REM Stop Zookeeper server
CALL C:\kafka\bin\windows\zookeeper-server-stop.bat
echo Zookeeper server stopped...


REM Remove the Kafka Streams state directory
RD /S /Q C:\dataset\tmp\kafka-streams\
echo Kafka Streams state directory removed...


REM Remove the Kafka and Zookeeper data directories
RD /S /Q C:\kafka\kafka-logs
RD /S /Q C:\kafka\kafka-logs-1
RD /S /Q C:\kafka\kafka-logs-2
RD /S /Q C:\kafka\zookeeper-data
echo Data directories removed...
REM Delete the topicCounter file if it exists
IF EXIST C:\dataset\topicCounter\topicCounter.txt del C:\dataset\topicCounter\topicCounter.txt && echo topicCounter.txt file deleted...

REM Attempt to stop and close Kafka consumers if they exist.
taskkill /f /fi "WindowTitle eq kafka-console-consumer*OutputTopicSynopsis1*" /im cmd.exe 2>NUL
if errorlevel 1 (
    echo No consumers for OutputTopicSynopsis1 found...
) else (
    echo Killed existing Kafka consumer for OutputTopicSynopsis1...
)

taskkill /f /fi "WindowTitle eq kafka-console-consumer*OutputTopicSynopsis2*" /im cmd.exe 2>NUL
if errorlevel 1 (
    echo No consumers for OutputTopicSynopsis2 found...
) else (
    echo Killed existing Kafka consumer for OutputTopicSynopsis2...
)

taskkill /f /fi "WindowTitle eq kafka-console-consumer*OutputTopicSynopsis3*" /im cmd.exe 2>NUL
if errorlevel 1 (
    echo No consumers for OutputTopicSynopsis3 found...
) else (
    echo Killed existing Kafka consumer for OutputTopicSynopsis3...
)


REM Start Zookeeper server in a new terminal
start /min cmd.exe /C "cd /d C:\kafka && bin\windows\zookeeper-server-start.bat config\zookeeper.properties"
echo Zookeeper server started...

REM Start Kafka server in a new terminal
start /min cmd.exe /C "cd /d C:\kafka && bin\windows\kafka-server-start.bat config\server.properties"
echo Kafka server 0 started...

REM Start Kafka server in a new terminal
start /min cmd.exe /C "cd /d C:\kafka && bin\windows\kafka-server-start.bat config\server-1.properties"
echo Kafka server 1 started...

REM Start Kafka server in a new terminal
start /min cmd.exe /C "cd /d C:\kafka && bin\windows\kafka-server-start.bat config\server-2.properties"
echo Kafka server 2 started...
