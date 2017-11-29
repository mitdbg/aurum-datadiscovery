package core.tasks;

import core.SourceType;
import inputoutput.connectors.Old_Connector;

public interface ProfileTask {

    public int getTaskId();

    public Old_Connector getConnector();

    public SourceType getSourceType();

    public void close();

}
