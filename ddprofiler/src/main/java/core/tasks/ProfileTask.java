package core.tasks;

import core.SourceType;
import core.config.sources.SourceConfig;
import inputoutput.connectors.Connector;

public interface ProfileTask {

    public SourceConfig getSourceConfig();

    public int getTaskId();

    public Connector getConnector();

    public SourceType getSourceType();

    public void close();

}
