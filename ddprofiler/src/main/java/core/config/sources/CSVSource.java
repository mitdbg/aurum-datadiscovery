package core.config.sources;

import com.fasterxml.jackson.annotation.JsonProperty;

import core.SourceType;

public class CSVSource implements SourceConfig {

    private String sourceName;

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

    @Override
    public String getSourceName() {
	return sourceName;
    }

    @Override
    public void setSourceName(String sourceName) {
	this.sourceName = sourceName;
    }

    @Override
    public SourceType getSourceType() {
	return SourceType.csv;
    }

}
