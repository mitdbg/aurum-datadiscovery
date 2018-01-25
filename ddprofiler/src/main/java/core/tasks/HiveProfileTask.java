package core.tasks;

import core.SourceType;
import core.config.sources.HiveSourceConfig;
import core.config.sources.SourceConfig;
import inputoutput.connectors.Connector;
import inputoutput.connectors.HiveConnector;

public class HiveProfileTask implements ProfileTask {

    private int taskId;
    private HiveConnector connector;
    private HiveSourceConfig config;

    public HiveProfileTask(HiveSourceConfig config) {
	this.config = config;
	String sourceName = config.getSourceName();
	String tableName = config.getRelationName();

	HiveConnector dbc = new HiveConnector(config);

	int id = ProfileTaskFactory.computeTaskId(sourceName, tableName);
	this.taskId = id;
	this.connector = dbc;
    }

    @Override
    public SourceConfig getSourceConfig() {
	return this.config;
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
    public SourceType getSourceType() {
	return SourceType.sqlserver;
    }

    @Override
    public void close() {
	this.connector.destroyConnector();
    }

}
