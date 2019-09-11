package com.github.hosnimed;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfluentKStreamsDemoTest {

        //Setup the Test Driver
        TopologyTestDriver testDriver;
        KeyValueStore<String, Integer> store;

        @Before
        public void setup() {
                Topology topology = new Topology();
                topology.addSource("sourceProcessor", "input-topic");
//                topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
                topology.addStateStore(
                        Stores.keyValueStoreBuilder(
                                Stores.inMemoryKeyValueStore("aggStore"),
                                Serdes.String(),
                                Serdes.Integer()).withLoggingDisabled(), // need to disable logging to allow store pre-populating
                        "aggregator");
                topology.addSink("sinkProcessor", "result-topic", "aggregator");

                // setup test driver
                Properties config = new Properties();
                config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "maxAggregation");
                config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
                config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
                testDriver = new TopologyTestDriver(topology, config);

                // pre-populate store
                store = testDriver.getKeyValueStore("aggStore");
                store.put("a", 21);
        }

        @Test
        public void demoTest() {
                /**
                 * Stream DSL API
                 */
                StreamsBuilder builder = new StreamsBuilder();
                Topology topologyDSL = builder.build();

                /**
                 * Steam Processor API
                 */
                Topology topologyProcessor = new Topology();

                /**
                 * Configuration properties
                 */
                Properties config = new Properties();
                config.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-test");
                config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");

                //Setup the Test Driver
                TopologyTestDriver testDriver = new TopologyTestDriver(topologyProcessor, config);
                /**
                 * The test driver accepts `ConsumerRecords` with key and value type``` byte[]```.
                 * Because ```byte[]``` types can be problematic, you can use the `ConsumerRecordFactory` to generate records by providing regular Java types for key and values and the corresponding serializers.
                 */
                ConsumerRecordFactory<String, Integer> factory = new ConsumerRecordFactory<>(
                        "streams-plaintext-input",
                        new StringSerializer(),
                        new IntegerSerializer()
                );
                testDriver.pipeInput(factory.create("streams-plaintext-input", "key", 1, 100));
                ProducerRecord<String, Long> outputRecord = testDriver.readOutput(
                        "streams-plaintext-output",
                        new StringDeserializer(),
                        new LongDeserializer()
                );

                OutputVerifier.compareKeyValue(outputRecord, "key", 100L);
        }

        @Test
        public void mock_processor_context() {
                final Processor processorUnderTest = new SimpleProcessor();
                final MockProcessorContext context = new MockProcessorContext();
                processorUnderTest.init(context);

                processorUnderTest.process("key","value");

                final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();
                assertTrue(forwarded.hasNext());
                assertEquals(forwarded.next(), new KeyValue<String, String>("key", "value"));

                context.resetForwards();
                assertEquals(context.forwarded().size(), 0);
        }

        @After
        public void tearDown() {
                testDriver.close();
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