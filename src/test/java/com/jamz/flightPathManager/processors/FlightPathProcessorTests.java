package com.jamz.flightPathManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jamz.flightPathManager.FlightPathManager;
import com.jamz.flightPathManager.serdes.JSONSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.jamz.flightPathManager.FlightPathManager.Constants.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;

public class FlightPathProcessorTests {

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, JsonNode> inputTopic;
    private static TestOutputTopic<String, JsonNode> outputTopic;
    private static KeyValueStore<String, JsonNode> flight_envelopes;
    private static final JsonNodeFactory nodeFactory = new JsonNodeFactory(true);

    @BeforeAll
    static void setup() {

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-flight_path-manager");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);

        // Build the topology using the prod code.
        Topology topology = FlightPathManager.buildTopology(props, true);

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(FLIGHT_PATH_TOPIC,
                Serdes.String().serializer(), jsonSerializer);
        outputTopic = testDriver.createOutputTopic(FLIGHT_PATH_TOPIC,
                Serdes.String().deserializer(), jsonDeserializer);

        flight_envelopes = testDriver.getKeyValueStore(FLIGHT_PATH_STORE_NAME);
    }

    @AfterAll
    static void tearDown() {
        testDriver.close();
    }

    // Provide an empty store for each test (with envelopes of course)
    @BeforeEach
    void emptyPathStore() {
        KeyValueIterator<String, JsonNode> all_envelopes = flight_envelopes.all();
        while (all_envelopes.hasNext()) {
            KeyValue<String, JsonNode> env = all_envelopes.next();
            ((ArrayNode) env.value).removeAll();
        }
    }

    @Test
    void testSinglePathRequest() {
        ObjectNode request = new ObjectNode(nodeFactory);
        request.put("eventType", "PathProposal")
                .put("start", 123)
                .put("end", 456); // These values don't matter in the context of this test

        inputTopic.pipeInput("0x1", request);

        assertFalse(outputTopic.isEmpty());

        KeyValue<String, JsonNode> result = outputTopic.readKeyValue();
        assertEquals("0x1", result.key);
        // Flight paths should be assigned with lowest path having the highest priority, so we ensure that the
        // path assigned was the lowest envelope.
        assertEquals(FlightPathProcessor.ENV_MINIMUM, result.value.get("altitude").intValue());
        assertEquals(1, flight_envelopes.get(String.valueOf(FlightPathProcessor.ENV_MINIMUM)).size());
        assertEquals("PathAssignment", result.value.get("eventType").textValue());
    }
}
