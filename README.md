# Correlation-CEP
A project containing the re-imagined version of the [Correlation Engine project](https://git.swisscom.com/projects/EC-ASR/repos/correlation-engine/browse). Contains of a data generation engine(for testing purpose) and a data correlation engine.


## Part 1: Generating Data
This project `./datagen` is for producing event data for tesing purpose, consists of a data generation engine and a Kafka producer instance:
![](docs/data_gen.png)

## Part 2: Correlation
This project `.correlation` has a legacy ksql implemenation and a Flink implemenation, current flink implementaiton has an overall architecture:
![](docs/flink_archi.png)
Data path is abstrated as a pipeline (implemented using data stream), and each data correlation business logic is abstracted as a service to be use ( implmented using processor function).
![](docs/detial_pipe.png)