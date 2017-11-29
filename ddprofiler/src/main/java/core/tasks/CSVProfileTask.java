package core.tasks;

import java.io.IOException;

import core.SourceType;
import core.config.sources.CSVSourceConfig;
import inputoutput.conn.Connector;
import inputoutput.conn.FileConnector;

public class CSVProfileTask implements ProfileTask {

    private int taskId;
    private Connector connector;

    public CSVProfileTask(CSVSourceConfig conf) {
	FileConnector fc = null;
	String sourceName = conf.getSourceName();
	String path = conf.getPath();
	String separator = conf.getSeparator();

	try {
	    fc = new FileConnector(sourceName, path, sourceName, separator);
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
    public Connector getConnector() {
	return connector;
    }

    @Override
    public void close() {
	this.connector.close();
    }

}
