package sources.tasks;

import sources.SourceType;
import sources.config.SourceConfig;
import sources.connectors.Connector;

public interface ProfileTask {

    public SourceConfig getSourceConfig();

    public int getTaskId();

    public Connector getConnector();

    public SourceType getSourceType();

    public void close();

}
