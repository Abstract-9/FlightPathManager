package com.jamz.flightPathManager.serdes;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

public final class JSONSerde extends Serdes.WrapperSerde<JsonNode> {
    public JSONSerde() {
        super(new JsonSerializer(), new JsonDeserializer());
    }
}
