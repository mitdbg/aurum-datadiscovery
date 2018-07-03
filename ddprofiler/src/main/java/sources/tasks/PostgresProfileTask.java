package sources.tasks;

import sources.SourceType;
import sources.config.PostgresSourceConfig;
import sources.config.SourceConfig;
import sources.connectors.Connector;
import sources.connectors.PostgresConnector;

public class PostgresProfileTask implements ProfileTask {

    private int taskId;
    private PostgresConnector connector;
    private PostgresSourceConfig config;

    public PostgresProfileTask(PostgresSourceConfig config) {
	this.config = config;

	String sourceName = config.getSourceName();
	String tableName = config.getRelationName();

	// Connection c = Old_DBConnector.getOrCreateConnector(sourceName,
	// DBType.POSTGRESQL, connIP, port, dbName, tableName,
	// username, password);

	// Old_DBConnector dbc = new Old_DBConnector(c, sourceName,
	// DBType.POSTGRESQL, connIP, port, dbName, tableName, username,
	// password);

	PostgresConnector dbc = new PostgresConnector(config);

	int id = ProfileTaskFactory.computeTaskId(sourceName, tableName);
	this.taskId = id;
	this.connector = dbc;
    }

    @Override
    public SourceConfig getSourceConfig() {
	return this.config;
    }

    @Override
    public SourceType getSourceType() {
	return SourceType.postgres;
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
	this.connector.destroyConnector();
    }

    @Override
    public String toString() {
	String sourceName = config.getSourceName() + "/" + config.getRelationName();
	return taskId + " - " + sourceName;
    }

}
