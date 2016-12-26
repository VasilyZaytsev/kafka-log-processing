# kafka-log-processing

## Task description (Requirements)
Написать Storm топологию **R3** для следующего сценария. 
На входе есть Kafka-топик **R1**, куда складываются некие логи. 
Формат сообщения в топике: 
JSON-список, в котором >= 0 записей, для каждой из записей ожидаются поля timestamp, host, level и text. **R2**
Топология скользящим окном длительностью 60 секунд высчитает среднюю частоту поступления (количество событий в секунду) **R4** и 
общее количество событий по окну для каждого из уровней: TRACE, DEBUG, INFO, WARN, ERROR **R5**. 
Эти величины пишутся по каждому хосту и уровню в HBase **R8**.
Если частота поступления по уровню ERROR превышает порог 1 событие в секунду, **R7** 
в Kafka-топик alerts необходимо записать событие об этом в формате JSON с полями host и error_rate.

Необходимо подумать, как обеспечить семантику exactly once для обработки событий лога, 
чтобы каждое событие лога в любой ситуации учитывалось ровно один раз (исключить дубликаты) **R6**. 

Работу системы необходимо покрыть автоматизированными тестами **R0**.

## How to 
### Run test
All test run in memory and automatically setup infrastructure (Zookeeper, Kafka, Storm) also in memory
Accordance to support docker console environment should be setup with docker settings 
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
     [2] ru.ps.onef.research.kafka.app.LogsProducerApp
    
    Enter number: > 2 (Secondly start producer)
    ```
    
### Configure 
For project configuration used [typesafe config](https://github.com/typesafehub/config)
Default [configuration file is reference.conf](./src/main/resources/reference.conf)

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
http://hbase-docker:16010/master-status

````
for exposed ports see run-image and dockerfile

**Inspired with:**
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

## What are done
* R1
* R2

