package core.config.sources;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Sources {

    @JsonProperty
    private int api_version;

    @JsonProperty
    private List<Source> sources;

    public int getApi_version() {
	return api_version;
    }

    public List<Source> getSources() {
	return sources;
    }

}
