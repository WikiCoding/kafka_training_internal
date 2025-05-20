# Repo containing the materials for a Kafka fundamentals training

To setup the Kafka broker run the following commands
```shell
./docker-compose-run.sh
```

To run the producer run: `mvn spring-boot:run -Pproducer`

To run the consumer run: `mvn spring-boot:run -Pconsumer`

To compile the protobuf contract run: `protoc -I=. --java-out=. Transaction.proto`

To use the Kafka-UI visit `localhost:8080`. Remember that the IP of the Advertised Listeners is hardcoded in the `docker-compose` file so make sure you put yours.
