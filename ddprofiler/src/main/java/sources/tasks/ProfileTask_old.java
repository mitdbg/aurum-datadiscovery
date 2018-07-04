package sources.tasks;

import sources.config.SourceConfig;
import sources.connectors.Connector;
import sources.main.SourceType;

public interface ProfileTask_old {

    public SourceConfig getSourceConfig();

    public int getTaskId();

    public Connector getConnector();

    public SourceType getSourceType();

    public void close();

}
