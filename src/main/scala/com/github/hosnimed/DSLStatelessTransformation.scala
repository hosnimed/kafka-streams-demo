package com.github.hosnimed

import java.time.Duration
import java.util.Properties

import DSLStatefulTransformation.config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object DSLStatelessTransformation extends App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  val config: Properties = {
    val p = new Properties()
    p.put(org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true.toString)

    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateless-stream-scala-application")
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long.getClass.getName)
    p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    p.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer")


    p
  }

  val producer = new KafkaProducer[String, Long](config)
  for (i <- 1 to 10 ){
    val record: ProducerRecord[String, Long] = new ProducerRecord[String, Long]("streams-stateless-input", if (i % 3 == 0) "a" else if (i%3 == 1) "b" else "c", i)
    producer.send(record)
  }

  val builder = new StreamsBuilder()
  val textLines: KStream[String, Long] = builder.stream[String, Long]("streams-stateless-input")//(consumed = Consumed[String, String]).`with`(Serdes.String, Serdes.String))

  /**
   * STREAM STATELESS TRANSFORMATIONS
   */
    val stream: KStream[String, String] = textLines.mapValues( (v:Long) => s"[$v]" )
    //branch
    val branches: Array[KStream[String, String]] = stream.branch(
      (k,v) => k.startsWith("a"), //1st predicate
      (k,v) => k.startsWith("b"), //2nd predicate
      (k,v) => true               //default
    )
   branches.foreach(_.peek( (_,_) => println("BRANCHING")).print(Printed.toSysOut()))

    //filter / filterNot
   val filetred = stream.filter((k,v) => k != "d")
//     filetred.peek((_,_) => println("FILTERING")).print(Printed.toSysOut())
    val filetredNot = stream.filter((k,v) => k == "d") //eq
//     filetredNot.peek((_,_) => println("FILTERING NOT")).print(Printed.toSysOut())
    //flatMap / flatMapValues
    val mapped: KStream[String, Int] = stream.flatMap { (k, v) =>
      var r = List.empty[(String, Int)]
      r = r.+:(k, v.length)
      r = r.+:(k, k.length)
      r
    }
//      mapped.peek((_,_) => println("MAPPING")).print(Printed.toSysOut())
  //peek
  //You would use peek to cause side effects based on the input data (similar to foreach)
  // and continue processing the input data (unlike foreach, which is a terminal operation).
//  stream.peek{ case (k,v) => println(s"$k:$v")}

  //foreach
//  stream.foreach{ case (k,v) => println(s"$k:$v")}

  // Group by the existing key, using the application's configured default serdes for keys and values.
   //GroupBy always causes data re-partitioning.
  val groupedStream = stream.groupBy( (k,v) => k)
  // GroupByKey is preferable to GroupBy
  // because it re-partitions data only if the stream was already marked for re-partitioning
  val groupedStream2 = stream.groupByKey
  // When the key and/or value types do not match the configured
  // default serdes, we must explicitly specify serdes.
//  val groupedStream = stream.groupByKey(Grouped.`with`(Serdes.ByteArray, /* key */ Serdes.String) /* value */)



  //Print
  stream.print(Printed.toSysOut())
  stream.print(Printed.toFile("target/streams.out").withLabel("streams"))

  stream.selectKey((k,v) => v.charAt(1)). peek((_,_) => println("SelectKey-Switching KV")).print(Printed.toSysOut())

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

  streams.cleanUp()
  streams.start()
  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

}
