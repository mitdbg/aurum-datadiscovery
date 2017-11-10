package core.config.sources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import core.SourceType;

public class Source {

    @JsonProperty
    private String name;

    @JsonProperty
    private SourceType type;

    @JsonProperty
    private JsonNode config;

    public String getName() {
	return name;
    }

    public SourceType getType() {
	return type;
    }

    public JsonNode getConfig() {
	return config;
    }

}
