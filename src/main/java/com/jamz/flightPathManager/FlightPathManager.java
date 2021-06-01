package com.jamz.flightPathManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.jamz.flightPathManager.processors.FlightPathProcessor;
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-landing-bay-manager");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "confluent:9092");
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

        streamBuilder.table(DRONE_STATUS_TOPIC, consumed, Materialized.as(DRONE_STORE_NAME));

        final Topology topBuilder = streamBuilder.build(props);

        topBuilder.addSource(FLIGHT_PATH_INPUT_NAME, FLIGHT_PATH_TOPIC);

        topBuilder.addProcessor(FLIGHT_PATH_PROCESSOR_NAME, FlightPathProcessor::new, FLIGHT_PATH_INPUT_NAME);

        topBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(FLIGHT_PATH_STORE_NAME),
                jsonSerde,
                jsonSerde
        ), FLIGHT_PATH_PROCESSOR_NAME);

        topBuilder.connectProcessorAndStateStores(FLIGHT_PATH_PROCESSOR_NAME, DRONE_STORE_NAME);

        topBuilder.addSink(FLIGHT_PATH_OUTPUT_NAME, FLIGHT_PATH_TOPIC);

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
