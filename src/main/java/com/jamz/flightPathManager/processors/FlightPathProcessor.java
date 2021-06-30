package com.jamz.flightPathManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import static com.jamz.flightPathManager.FlightPathManager.Constants.*;

/**
 * Tbh, this whole class is clunky af right now. Lots of optimization required. I'd say its good for < 10 drones.
 */
public class FlightPathProcessor implements Processor<String, JsonNode, String, JsonNode> {

    // Define minimum flight envelope, envelope interval, and maximum
    public static final int ENV_MINIMUM = 80, ENV_INTERVAL = 10, ENV_MAXIMUM = 200;

    private KeyValueStore<String, JsonNode> pathStore;
    private ProcessorContext<String, JsonNode> context;
    private final JsonNodeFactory factory = new JsonNodeFactory(true);


    @Override
    public void init(ProcessorContext<String, JsonNode> context) {
        Processor.super.init(context);
        this.context = context;
        pathStore = context.getStateStore(FLIGHT_PATH_STORE_NAME);

        if (pathStore.approximateNumEntries() == 0) {
            // Initialize store
            for(int i=ENV_MINIMUM; i<ENV_MAXIMUM; i+=ENV_INTERVAL) {
                pathStore.put(String.valueOf(i), new ArrayNode(factory));
            }
        }
    }

    @Override
    public void process(Record<String, JsonNode> record) {
        String eventType = record.value().get("eventType").textValue();
        if (!eventType.equals("PathProposal") &&
            !eventType.equals("PathCompletion")) return;

        if(eventType.equals("PathProposal")) {
            // Try to find a usable flight envelope.
            int envelope = findFlightEnvelope(record.value());

            // The envelope value will be null if we cant find an empty one.
            if (envelope != -1) {
                // First, put the path into the envelope
                ArrayNode storeEnv = (ArrayNode) this.pathStore.get(String.valueOf(envelope));
                ObjectNode path = ((ObjectNode) record.value().deepCopy()).put("drone_id", record.key())
                        .without("eventType");
                storeEnv.add(path);
                this.pathStore.put(String.valueOf(envelope), storeEnv);
                // Now we generate the result and emit it
                ObjectNode result = new ObjectNode(factory);
                result.put("eventType", "PathAssignment")
                        .put("altitude", envelope)
                        .set("start", record.value().get("start"));
                result.set("end", record.value().get("end"));
                this.context.forward(new Record<String, JsonNode>(record.key(), result, System.currentTimeMillis()));
            }
        } else {
            removeFlightPath(record.key());
        }

    }

    @Override
    public void close() {
        Processor.super.close();
    }

    private int findFlightEnvelope(JsonNode event) {
        JsonNode start = event.get("start"), end = event.get("end");

        int leastPaths = Integer.MAX_VALUE, leastPathsKey = -1;

        // Try to place the drone in an envelope, prioritizing lower ones first
        for (int i=ENV_MINIMUM; i<ENV_MAXIMUM; i+=ENV_INTERVAL) {
            ArrayNode paths = (ArrayNode) pathStore.get(String.valueOf(i));
            if (paths.isEmpty()) return i;
            else {
                if(paths.size() < leastPaths) {
                    leastPaths = paths.size();
                    leastPathsKey = i;
                }
            }
        }

        // Since there aren't any flight envelopes with no active paths, we need to calculate a non-intersecting path
        // Arrangement. As we'll be flying with less drones than available envelopes for now, I'll leave this as a TODO
        return leastPathsKey;
    }

    private void removeFlightPath(String droneID) {
        // TODO This is clunky af, gotta fix the data structure in the future.
        for (KeyValueIterator<String, JsonNode> it = pathStore.all(); it.hasNext(); ) {
            KeyValue<String, JsonNode> envelope = it.next();
            ArrayNode paths = (ArrayNode) envelope.value;
            if (!paths.isEmpty()) {
                for (int i = 0; i < paths.size(); i++) {
                    if (paths.get(i).get("drone_id").textValue().equals(droneID)) {
                        paths.remove(i);
                        break;
                    }
                }
            }
        }
    }
}
