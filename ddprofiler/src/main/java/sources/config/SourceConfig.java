package sources.config;

import sources.main.SourceType;

public interface SourceConfig {

    public SourceType getSourceType();

    public String getSourceName();

    public void setSourceName(String sourceName);

    public String getRelationName();

    public String getPath();

    public SourceConfig selfCopy();

}
