# kafka-log-processing

## Task description (Requirements)
Написать Storm топологию **R3** для следующего сценария. 
На входе есть Kafka-топик **R1**, куда складываются некие логи. 
Формат сообщения в топике: 
JSON-список, в котором >= 0 записей, для каждой из записей ожидаются поля timestamp, host, level и text. **R2**
Топология скользящим окном длительностью 60 секунд высчитает 
среднюю частоту поступления (количество событий в секунду) **R4** и 
общее количество событий по окну для каждого из уровней: TRACE, DEBUG, INFO, WARN, ERROR **R5**. 
Эти величины пишутся по каждому хосту и уровню в HBase **R8**.
Если частота поступления по уровню ERROR превышает порог 1 событие в секунду, **R7** 
в Kafka-топик alerts необходимо записать событие об этом в формате JSON с полями host и error_rate.

Необходимо подумать, как обеспечить семантику exactly once для обработки событий лога, 
чтобы каждое событие лога в любой ситуации учитывалось ровно один раз (исключить дубликаты) **R6**. 

Работу системы необходимо покрыть автоматизированными тестами **R0**.

## What are done
* R1
* R2
* R4
* R5
* R8
* R0
* R3
* R7

### HBase

#### Docker
**Build:**
````
docker build -t local-hbase-img .
````

**Run:**
Before run you should update hosts with alias
````
        192.168.99.100  hbase-docker
````
 Where 192.168.99.100 is a 
```` 
    > docker-machine ip default  
````
and default is a target docker machine

Also you provide shared directory
http://stackoverflow.com/questions/33312662/docker-toolbox-mount-file-on-windows
````
run-image
````

**Connect:**
````
docker run --rm -it --link IMAGE_ID:hbase-docker local-hbase hbase shell
docker run --rm -it --link test_local_hbase:hbase-docker local-hbase-img hbase shell

docker run --rm -it --link vz_local_hbase:hbase-docker vasilyzaytsev/local-hbase hbase shell
scan 'logs_statistics', {LIMIT => 1, ROWPREFIXFILTER => 'www.example.com' }
http://stackoverflow.com/questions/39772832/hbase-row-prefix-scan-in-reverse-order-in-hbase

http://hbase-docker:16010/master-status

````
for exposed ports see run-image and dockerfile

**Inspired by:**
https://github.com/nerdammer/dockers/tree/master/hbase-1.1.0.1
https://github.com/dajobe/hbase-docker

**FIXES**
https://github.com/docker/kitematic/issues/519
docker-machine regenerate-certs default

https://github.com/docker/docker/issues/27734
Update to VirtualBox Version 5.1.10 

https://github.com/docker/toolbox/issues/80
Fixed with VB update and set volume like 
/c/Users/Vasily.Zaytsev/docker/valume2
Payattention that folder should be placed at user home

**IMPORTANT!!!**
Don't use URI for HBase directory path's
WRONG
```
     <name>hbase.rootdir</name>
     <value>file:////data/hbase</value>
```
Correct
```
     <name>hbase.rootdir</name>
     <value>/data/hbase</value>
```

## How to 
### Run test
All test run in memory and automatically setup infrastructure (Zookeeper, Kafka, Storm) also in memory
Accordance to support docker console environment should be setup with docker settings
HBase also will be started automatically but docker machine should be started and 
test should be started from console with docker environment awareness. As described in HBase Run section.   
 
```
sbt test
```

### Start environment

1. Start Zookeeper

    ````
    cd %KAFKA_HOME
    > bin\windows\zookeeper-server-start.bat config\zookeeper.properties
    ````
1. Start Kafka

    ````
    cd %KAFKA_HOME
    > dir bin\windows\kafka-server-start.bat config\server.properties
    ````
    
### Run application 
Before run applications environment should be started 

1. From project root folder

    ```
    > sbt run
    Multiple main classes detected, select one to run:
    
     [1] ru.ps.onef.research.kafka.app.LogsConsumerApp
     [2] ru.ps.onef.research.kafka.app.LogsProducerApp
    
    Enter number: > 1 (Firstly start consumer)
    
    > sbt run
    Multiple main classes detected, select one to run:
    
     [1] ru.ps.onef.research.kafka.app.LogsConsumerApp
     [2] ru.ps.onef.research.afka.app.LogsProducerApp
    
    Enter number: > 2 (Secondly start producer)
    ```
    
### Configure 
For project configuration used [typesafe config](https://github.com/typesafehub/config)
Default [configuration file is reference.conf](./src/main/resources/reference.conf)

## Test results
### DataSet 

| File name | Creation | Size | Messages |
| --------- | -------- | ---- | -------- |
| dataset.txt | Jan 13 16:30 | 570M | 4504736 |

### Execution results
All results is avarege between 3 execution

| Messages | Parameters   | Time taked | Rate | Windows count | Comment |
| -------- | ------------ | ---------- | ---- | ------------- | ------- |
| 4504736 | 2G | 144 | 26302 | 234 | Without alerting + println |
| 4504736 | 2G | 138 | 28378 | 306 | With simple alerting |
| 4504736 | 4G | 125 | 31254 | 246 | With simple alerting more stable result |
| 4504736 | 8G | 95 | 38419 | 198 | With simple alerting actual memory usage 4 GB |
| 4504736 | 8G G1 | 115 | 33274 | 234 | With simple alerting actual memory usage 3.4 - 5.4 GB |
| 4504736 | 4G G1 Compressed | 123 | 30501 | 252 | With simple alerting actual memory usage 3.3 GB |
| 4504736 | 8G G1 Compressed | 117 | 32390 | 234 | With simple alerting actual memory usage 4.2 GB |

On all test CPU load closest to 100%