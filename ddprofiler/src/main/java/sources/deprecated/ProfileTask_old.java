package sources.deprecated;

import sources.SourceType;
import sources.config.SourceConfig;

public interface ProfileTask_old {

    public SourceConfig getSourceConfig();

    public int getTaskId();

    public Connector getConnector();

    public SourceType getSourceType();

    public void close();

}
