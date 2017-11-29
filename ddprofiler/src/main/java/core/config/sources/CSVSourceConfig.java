package core.config.sources;

import com.fasterxml.jackson.annotation.JsonProperty;

import core.SourceType;

public class CSVSourceConfig implements SourceConfig {

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

    public void setPath(String path) {
	this.path = path;
    }

    public void setSeparator(String separator) {
	this.separator = separator;
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
