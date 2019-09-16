package com.github.hosnimed

import java.time.Duration
import java.util.{Collections, Locale}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier, PunctuationType}
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore, StoreBuilder, Stores}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig, Topology}

import scala.language.postfixOps

object WordCountProcessorExample extends App with ConfigHelper {
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-scala-wordcount-processor")

  // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
  config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  class WordCountProcessor extends Processor[String, String] {
    var context: ProcessorContext = _
    var kvStore: KeyValueStore[String, Long] = _

    override def init(context: ProcessorContext): Unit = {
      this.context = context
      this.kvStore = context.getStateStore("wc").asInstanceOf[KeyValueStore[String,Long]]
      // schedule a punctuate() method every second based on event-time
      this.context.schedule(Duration.ofMillis(1), PunctuationType.STREAM_TIME, _ =>
        kvStore.all().forEachRemaining { case pair : KeyValue[String,Long] =>
          println(s"Context forward = ${pair.key}:${pair.value}")
          context.forward(pair.key, pair.value.toString)
        }
      )
      // commit progress
      this.context.commit()
    }

    override def process(key: String, value: String): Unit = {
      val words: Array[String] = value.toLowerCase(Locale.getDefault).split("\\s")
      words.foreach(w => {
        this.kvStore.get(w) match {
          case oldValue:Long if oldValue != null=> println(s"Get Value form KeyValueStore for : $w => $oldValue")
            this.kvStore.put(w, oldValue + 1)
          case _ => println(s"Get form KeyValueStore for : $w")
            this.kvStore.put(w, 1)
        }
      })
    }

    override def close(): Unit = {
      val iter: KeyValueIterator[String, Long] = this.kvStore.all()
      while(iter.hasNext) {
        val pair = iter.next()
        println(s"==> ${pair.key}::${pair.value}")
      }
      this.kvStore.all().close()
      }
  }
   class WordCountProcessorSupplier extends ProcessorSupplier[String, String]{
     override def get(): Processor[String, String] = new WordCountProcessor().asInstanceOf[Processor[String,String]]
   }

  val topology = new Topology()
  private val input = "wc-stream-processor-input"
  private val output = "wc-stream-processor-output"
  createTopics(Iterable(input, output))

  val producer = new KafkaProducer[String, String](config)
  var record = new ProducerRecord[String,String](input, "", "")
  for(i <- 1 to 5) {
    val value = if (i%2==0) "Hello Kafka" else "Kafka Streams"
    record = new ProducerRecord[String,String](input, i % 2 toString, value)
    producer.send(record)
  }

  topology.addSource("Source", input)

  private val supplier = new WordCountProcessorSupplier()
  topology.addProcessor("Processor", supplier , "Source")
  private val inMemorySS: StoreBuilder[KeyValueStore[String, Long]] = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("wc"), Serdes.String, Serdes.Long)
  private val persistentFaultTolerantSS: StoreBuilder[KeyValueStore[String, Long]] = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("wc"), Serdes.String, Serdes.Long).withLoggingEnabled(Collections.singletonMap("min.insync.replicas", "1"))
  private val persistentNonFaultTolerantSS: StoreBuilder[KeyValueStore[String, Long]] = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("wc"), Serdes.String, Serdes.Long).withLoggingDisabled()
  topology.addStateStore( inMemorySS, "Processor")

  topology.addSink("Sink", output, "Processor")
  println(topology.describe())

  val streams = new KafkaStreams(topology, config)

  streams.start()
  sys.ShutdownHookThread {
//    deleteTopics(Iterable(input,output))
    streams.close()
  }
}
