package core.tasks;

import core.SourceType;
import inputoutput.conn.Connector;

public interface ProfileTask {

    public int getTaskId();

    public Connector getConnector();

    public SourceType getSourceType();

    public void close();

}
