package core.tasks;

import java.io.IOException;

import core.SourceType;
import core.config.sources.CSVSourceConfig;
import inputoutput.connectors.Old_Connector;
import inputoutput.connectors.Old_FileConnector;

public class CSVProfileTask implements ProfileTask {

    private int taskId;
    private Old_Connector connector;

    public CSVProfileTask(CSVSourceConfig conf) {
	Old_FileConnector fc = null;
	String sourceName = conf.getSourceName();
	String path = conf.getPath();
	String relationName = conf.getRelationName();
	String separator = conf.getSeparator();

	try {
	    fc = new Old_FileConnector(sourceName, path, relationName, separator);
	} catch (IOException e) {
	    e.printStackTrace();
	}
	int id = ProfileTaskFactory.computeTaskId(path, sourceName);
	this.taskId = id;
	this.connector = fc;
    }

    @Override
    public SourceType getSourceType() {
	return SourceType.csv;
    }

    @Override
    public int getTaskId() {
	return taskId;
    }

    @Override
    public Old_Connector getConnector() {
	return connector;
    }

    @Override
    public void close() {
	this.connector.close();
    }

    @Override
    public String toString() {
	String sourceName = connector.getSourceName();
	return taskId + " - " + sourceName;
    }

}
