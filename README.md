# Kafka Streams Examples

This project contains code examples based on [Confluent Kafka-Streams-Examples](https://github.com/confluentinc/kafka-streams-examples)
> # Build and install common locally
> $ mvn -DskipTests=true clean install

## Using IntelliJ or Eclipse

If you are using an IDE and import the project you might end up with a "missing import / class not found" error.
Some Avro classes are generated from schema files and IDEs sometimes do not generate these classes automatically.
To resolve this error, manually run:

```shell
$ mvn -Dskip.tests=true compile
```
