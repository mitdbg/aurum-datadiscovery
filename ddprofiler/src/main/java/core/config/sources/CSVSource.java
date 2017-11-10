package core.config.sources;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CSVSource {

    @JsonProperty
    private String path;

    @JsonProperty
    private String separator;

    public String getPath() {
	return path;
    }

    public String getSeparator() {
	return separator;
    }

}
