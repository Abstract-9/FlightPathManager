package com.jamz.flightPathManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.jamz.flightPathManager.processors.FlightPathProcessor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;

import static com.jamz.flightPathManager.FlightPathManager.Constants.*;
import com.jamz.flightPathManager.serdes.JSONSerde;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class FlightPathManager {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-flight_path-manager");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-419q3.us-east4.gcp.confluent.cloud:9092");
        // Security Config
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"PSAFM6VH7LWNGV5D\" password=\"DfAiu9RSyI/udfvUm9j3HUtxHEECfrR9+K7tE8NTCI5g1x2am9ZkRfFWUSf+uT8G\";");
        // Performance Config
        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), 2147483647);
        props.put("producer.confluent.batch.expiry.ms", 9223372036854775807L);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 300000);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 9223372036854775807L);
        // Serdes Config
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);

        final Topology topology = buildTopology(props, false);
        run_streams(topology, props);
    }

    public static Topology buildTopology(Properties props, boolean testing) {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final Consumed<String, JsonNode> consumed = Consumed.with(Serdes.String(), jsonSerde);

        final StreamsBuilder streamBuilder = new StreamsBuilder();

        // Don't need this for now
        // streamBuilder.table(DRONE_STATUS_TOPIC, consumed, Materialized.as(DRONE_STORE_NAME));

        final Topology topBuilder = streamBuilder.build(props);

        topBuilder.addSource(FLIGHT_PATH_INPUT_NAME, FLIGHT_PATH_TOPIC);

        topBuilder.addProcessor(FLIGHT_PATH_PROCESSOR_NAME, FlightPathProcessor::new, FLIGHT_PATH_INPUT_NAME);

        // Logging is disabled on all state stores for testing. This will need to be different in prod
        topBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(FLIGHT_PATH_STORE_NAME),
                Serdes.String(),
                jsonSerde
        ).withLoggingDisabled(), FLIGHT_PATH_PROCESSOR_NAME);

        // topBuilder.connectProcessorAndStateStores(FLIGHT_PATH_PROCESSOR_NAME, DRONE_STORE_NAME);

        topBuilder.addSink(FLIGHT_PATH_OUTPUT_NAME, FLIGHT_PATH_TOPIC, FLIGHT_PATH_PROCESSOR_NAME);

        return topBuilder;
    }

    private static void run_streams(Topology topology, Properties props) {
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        streams.start();
    }

    // The kafka streams processor api uses a lot of internal names, and we need to refer to a few external topics.
    // This just helps keep everything in one place.
    public static class Constants {
        // Topics
        public static final String DRONE_STATUS_TOPIC = "DroneStatus";
        public static final String FLIGHT_PATH_TOPIC = "FlightPath";

        // Internal names
        public static final String FLIGHT_PATH_INPUT_NAME = "FlightPathInput";
        public static final String FLIGHT_PATH_OUTPUT_NAME = "FlightPathOutput";
        public static final String FLIGHT_PATH_PROCESSOR_NAME = "FlightPathProcessor";
        public static final String FLIGHT_PATH_STORE_NAME = "FlightPathStore";
        public static final String DRONE_STORE_NAME = "DroneStateStore";

    }
}
