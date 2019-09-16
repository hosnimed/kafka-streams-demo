# Kafka Streams Examples

This project contains code examples based on [Confluent Kafka-Streams-Examples](https://github.com/confluentinc/kafka-streams-examples)
### Build and install common locally
```shell
$ mvn -DskipTests=true clean install
```

### Run an example
```shell 
$ mvn -DskipTests=true exec:java -Dexec.mainClass=com.github.hosnimed.EXAMPLE_CLASS_NAME -Dexec.args="APP_ARGS"
$ mvn -DskipTests=true exec:java -Dexec.mainClass=com.github.hosnimed.StreamToStreamJoinExample -Dexec.args="left"
```

## Using IntelliJ or Eclipse

If you are using an IDE and import the project you might end up with a "missing import / class not found" error.
Some Avro classes are generated from schema files and IDEs sometimes do not generate these classes automatically.
To resolve this error, manually run:

```shell
$ mvn -Dskip.tests=true compile
```

## Reset Application's local state
You can reset an application and force it to reprocess its data from scratch by using [the application reset tool](https://docs.confluent.io/current/streams/developer-guide/app-reset-tool.html#streams-developer-guide-app-reset). 
This can be useful for development and testing, or when fixing bugs.

```shell
$ kafka-streams-application-reset --application-id stream-scala-join \
  --input-topics join-input-1, join-input-2 \
  --intermediate-topics temp-table-topic-1 temp-table-topic-2
```