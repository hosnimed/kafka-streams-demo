package com.github.hosnimed

import scala.language.postfixOps
import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams._
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.kstream.{Printed, Reducer, SessionWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.WindowStore

object DSLStatefulTransformation extends App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  val exceptionHandler = new LogAndContinueExceptionHandler()
  val config: Properties = {
    val p = new Properties()
    p.put(org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true.toString)

    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful-stream-scala-application")
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    p.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, exceptionHandler.getClass.getName)

    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long.getClass.getName)

    p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    p.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer")
    p
  }

  val producer = new KafkaProducer[String, Long](config)
  for (i <- 1 to 10 ){
    val record: ProducerRecord[String, Long] = new ProducerRecord[String, Long]("streams-stateful-input", (i % 3 toString), i)
    producer.send(record)
  }

  val builder = new StreamsBuilder()
  val textLines: KStream[String, Long] = builder.stream[String, Long]("streams-stateful-input")//(consumed = Consumed[String, String]).`with`(Serdes.String, Serdes.String))

  /**
   * STREAM STATEFUL TRANSFORMATIONS
   */
/*
      val stream: KStream[String, String] = textLines.map{ (_, line) =>
      val arr: Array[String] = line.toLowerCase.split(":")
      println(arr.mkString)
      (arr(0), arr(1))
    }
*/
  // count
  val wordCount = textLines
      .groupBy((k, _) => k) // KGroupedStream[String, String]
//      .groupByKey //eq
      .count() // KTable[String, Long]
      .toStream // KStream[String, Long]
  /**
   * Simple Aggregation
   * */
  // Aggregating a KGroupedStream (note how the value type changes from String to Long)
  val groupedStream: KGroupedStream[String, Long] = textLines.groupByKey
  val aggregatedStream: KTable[String, Long] = groupedStream
      .aggregate(initializer = 0L)( (aggKey:String, newValue:Long, aggValue:Long) => (aggValue + newValue) )
//                                  (Materialized.as("aggregate-stream-store").withValueSerde(org.apache.kafka.streams.scala.Serdes.Long))

  /**
   * Windowed Aggregation
   * */
  /** Tumbling window **/
  // Aggregating with time-based windowing (here: with 5-minute tumbling windows)
  val timeWindowedAggregatedStream: KTable[Windowed[String], Long] = groupedStream
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).grace(Duration.ofMinutes(3)))
    .count()
  /** Session window **/
  // Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
  val sessionWindowedAggregatedStream = groupedStream
        .windowedBy(SessionWindows.`with`(Duration.ofSeconds(5)))
      .count()

  private val stream: KStream[String, Long] = textLines.mapValues(_.toLong)
  /**
   * Reducing
   * */
  //Reducing KGroupedStream
  private val kGroupedStream = stream.groupByKey
  val aggregateStream = kGroupedStream
    .reduce( (aggValue:Long, newValue:Long) => aggValue + newValue /*adder*/  )
  //Reducing KGroupedTable
  /**
   * Convert a KStream to a KTable without an aggregation
   * Option 1: Write KStream to Kafka, read back as KTable
   * @link: https://docs.confluent.io/current/streams/faq.html#how-can-i-convert-a-kstream-to-a-ktable-without-an-aggregation-step
   * */

  stream.to("temp-topic")//(Produced.`with`(Serdes.String, Serdes.Long))
  val table: kstream.KTable[String, Long] = builder.table("temp-topic", Consumed.`with`(Serdes.String, Serdes.Long))
  private val kGroupedTable: kstream.KGroupedTable[String, Long] = table.groupBy( (k:String, v:Long) => (k,v))

  val aggregateTable = kGroupedTable
    .reduce(
        (aggValue:Long, newValue:Long) => aggValue + newValue /*adder*/ ,
        (aggValue:Long, oldValue:Long) => aggValue - oldValue /*subtractor*/ ,
      )
  /*
  */
  //Print
//  textLines.print(Printed.toSysOut())

  private val windowMapper: (Windowed[String], Long) => (String, Long) = (w: Windowed[String], v: Long) => (s"${w.key()}@[${w.window().start()} : ${w.window().end()}]", v)
  timeWindowedAggregatedStream.toStream.map(windowMapper)
    .peek( (k,_) => println(s"================================ Stream = TimeWindow = WindowAggregation For  $k =======================================") )
    .print(Printed.toSysOut())
  sessionWindowedAggregatedStream.toStream.map(windowMapper)
    .peek( (k,_) => println(s"================================ Table = SessionWindow = WindowAggregation For $k =======================================") )
    .print(Printed.toSysOut())

  aggregatedStream.toStream
    .peek( (k,_) => println(s"================================ Stream = Aggregation For  $k =======================================") )
    .print(Printed.toSysOut())
  aggregateTable.toStream
    .peek( (k,_) => println(s"================================ Table = Aggregation For $k =======================================") )
    .print(Printed.toSysOut())

  //  aggregatedStream.toStream.print(Printed.toFile("target/streams.out").withLabel("streams"))


  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

  streams.cleanUp()
  streams.start()
  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

}
