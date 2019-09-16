package com.github.hosnimed

import java.util.Properties
import collection.JavaConverters._
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.Serdes

trait ConfigHelper {
  this: App =>

  val config: Properties = {
    val p = new Properties()
    p.put("log.retention.minutes", (24 * 60).toString)
    p.put("auto.offset.reset", "latest")

    p.put(org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false.toString)

    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-scala-join")
    val bootstrapServers = "localhost:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    //    Enable Optimizations
    p.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE)

    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long.getClass.getName)
    p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    p.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer")


    p
  }
  val adminClient: AdminClient = AdminClient.create(config)

  def createTopics(topics: Iterable[String]) = {
    val newTopics = topics.map(t => new NewTopic(t, 1, 1))
    adminClient.createTopics(newTopics.asJavaCollection).all()
      .thenApply { _ =>
        Console.err.println("Topic created!")
        //        deleteTopics(newTopics)
      }.isDone
  }

  def deleteTopics(topics: Iterable[String]) = {
    val newTopics = topics.map(t => new NewTopic(t, 1, 1))
    val result = adminClient.deleteTopics(newTopics.map(_.name()).asJavaCollection)
    val isDone = result
      .all()
      .whenComplete((_, _) =>
        Console.err.println("Delete Completed!")

      )
    //    val msg = if (isDone) "Topic Deleted :)" else "Delete Error :("
    //    Console.err.println(msg)
    isDone
  }
}
