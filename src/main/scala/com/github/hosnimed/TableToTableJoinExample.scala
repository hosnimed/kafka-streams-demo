package com.github.hosnimed

import java.time.Duration

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{JoinWindows, Printed}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._

import scala.concurrent.Future
import scala.util.Try

object TableToTableJoinExample extends App with ConfigHelper {
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-scala-table-to-table-join-example")

  val joinType: JoinType = if (args.length > 0) {
    args(0) match {
      case "inner" => InnerJoin
      case "left" => LeftJoin
      case "outer" => OuterJoin
      case _ => InnerJoin
    }
  } else {
    InnerJoin
  }
  Console.err.println(s"Jointype : $joinType")

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  sealed trait JoinType

  final case object InnerJoin extends JoinType

  final case object LeftJoin extends JoinType

  final case object OuterJoin extends JoinType


  val builder = new StreamsBuilder()


  val outputTopic = s"join-output"
  //  Console.err.println(outputTopic)
  //  deleteTopics(Iterable("join-input-1", "join-input-2", outputTopic))
  //  createTopics(Iterable(outputTopic))


  val producer = new KafkaProducer[String, Long](config)
  for (j <- 1 to 2 by 1) {
    val topic = s"join-input-$j"
    for (i <- 1 to 2) {
      val record: ProducerRecord[String, Long] = new ProducerRecord[String, Long](topic, if (i % 2 == 0) "a" else "b", i)
      producer.send(record)
    }
  }

  val stream1: KStream[String, Long] = builder.stream[String, Long]("join-input-1")
  val stream2: KStream[String, Long] = builder.stream[String, Long]("join-input-2")
  val table1: KTable[String, Long] = {
    val tempTopic = "temp-table-topic-1"
    deleteTopics(Iterable(tempTopic)).whenComplete { (_, throwable) =>
      Either.cond(throwable == null, createTopics(Iterable(tempTopic)), println(throwable.getLocalizedMessage))
    }
    stream1.to(tempTopic)
    val tempTable = builder.table[String, Long](tempTopic)
    tempTable
  }
  val table2: KTable[String, Long] = {
    val tempTopic = "temp-table-topic-2"
    deleteTopics(Iterable(tempTopic)).whenComplete { (_, throwable) =>
      Try {
        createTopics(Iterable(tempTopic))
      }.recover {
        case _ =>
          Console.err.println(throwable.getLocalizedMessage)
      }
    }
    stream2.to(tempTopic)
    val tempTable = builder.table[String, Long](tempTopic)
    tempTable
  }

  def showTable[K, V](tables: KTable[K, V]*) = {
    tables.foreach { s =>
      println(s.toString)
      s.toStream.foreach((k, v) => println(s"[$k : $v]"))
    }
  }

  implicit val ec = scala.concurrent.ExecutionContext.global

  /*
  Future.sequence(Seq(
    Future (showStream(stream1)),
    Future (showStream(stream2)),
    Future (StreamToStreamJoin)
  )).isCompleted
*/

  Future {
    println("======> Sources Begin")
    showTable(table1, table2)
    "PASS"
  }.andThen {
    case pass if pass.equals("PASS") => {
      println("======> Join Begin")
      TableToTableJoin()
    }
  }

  private def TableToTableJoin(joinType: JoinType = InnerJoin) = {
    val join: KTable[String, Long] = joinType match {
      case InnerJoin => table1.join(table2)((v1, v2) => v1 + v2)
      case LeftJoin => table1.leftJoin(table2)((v1, v2) => v1 + v2)
      case OuterJoin => table1.outerJoin(table2)((v1, v2) => v1 + v2)
      case _ => table1.join(table2)((v1, v2) => v1 + v2)
    }
    join.toStream
      .peek((k, _) => println(s"=====================Table JOIN Table For Key : $k ====================="))
      .print(Printed.toSysOut())
    join
  }

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.cleanUp()
  streams.start()
  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    purgeTopics
    adminClient.close(Duration.ofSeconds(10))
  }

  private def purgeTopics = {
    println(s"Purge Topics......")
    deleteTopics(Iterable("join-input-1", "join-input-2", outputTopic))
  }


}
