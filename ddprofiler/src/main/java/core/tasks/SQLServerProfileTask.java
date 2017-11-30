package core.tasks;

import core.SourceType;
import core.config.sources.SQLServerSourceConfig;
import core.config.sources.SourceConfig;
import inputoutput.connectors.Connector;
import inputoutput.connectors.SQLServerConnector;

public class SQLServerProfileTask implements ProfileTask {

    private int taskId;
    private SQLServerConnector connector;
    private SQLServerSourceConfig config;

    public SQLServerProfileTask(SQLServerSourceConfig config) {
	this.config = config;
	String sourceName = config.getSourceName();
	String tableName = config.getRelationName();

	// Connection c = Old_DBConnector.getOrCreateConnector(sourceName,
	// DBType.SQLSERVER, connIP, port, dbName, tableName,
	// username, password);
	//
	// Old_DBConnector dbc = new Old_DBConnector(c, sourceName,
	// DBType.SQLSERVER, connIP, port, dbName, tableName, username,
	// password);

	SQLServerConnector dbc = new SQLServerConnector(config);

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
