package com.github.hosnimed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;


public class ConfluentKStreamsDemo {

    public static void main(String[] args) {
        /**
         * Stream DSL API
         */
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("streams-plaintext-input").to("streams-plaintext-output");
        Topology topologyDSL = builder.build();

        /**
         * Steam Processor API
         */
        Topology topologyProcessor = new Topology();
        topologyProcessor.addSource("sourceProcessor", "streams-plaintext-input");

        class SimpleSupplier implements ProcessorSupplier<String, Integer> {
            @Override
            public Processor<String, Integer> get() {
            return new SimpleProcessor();
            }

        class SimpleProcessor implements Processor<String, Integer> {
            @Override
            public void init(ProcessorContext context) {
                System.err.println("init");
            }
            @Override
            public void process(String key, Integer value) {
                System.err.println("process "+key+"  :  "+value);
            }
            @Override
            public void close() {
                System.err.println("close");
            }
        }

        }

        topologyProcessor.addProcessor("processor", new SimpleSupplier(), "sourceProcessor");
        topologyProcessor.addSink("sinkProcessor", "streams-plaintext-output", "processor");

        /**
         * Configuration properties
         */
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        /**
         * Create the Streams
         */
        KafkaStreams streams = new KafkaStreams(topologyDSL, config);

        /**
         * Start the Streams
         */
        streams.start();


        /**
         * ******************************************************************
         */

        /**
         * To catch any unexpected exceptions, you can set an java.lang.Thread.UncaughtExceptionHandler before you start the application.
         * This handler is called whenever a stream thread is terminated by an unexpected exception:
         */
/*
        streams.setStateListener((newState, oldState) -> newState = KafkaStreams.State.CREATED);
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
                    // here you should examine the throwable/exception and perform an appropriate action!
                }
        );
*/
/**
 *
 //Stop the Kafka Streams threads
 streams.close();
 */
        /**
         Add shutdown hook to stop the Kafka Streams threads.
         You can optionally provide a timeout to `close`.
         */
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
