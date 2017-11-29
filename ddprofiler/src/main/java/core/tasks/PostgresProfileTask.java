package core.tasks;

import java.sql.Connection;

import core.SourceType;
import core.config.sources.PostgresSourceConfig;
import inputoutput.connectors.DBType;
import inputoutput.connectors.Old_Connector;
import inputoutput.connectors.Old_DBConnector;

public class PostgresProfileTask implements ProfileTask {

    private int taskId;
    private Old_Connector connector;

    public PostgresProfileTask(PostgresSourceConfig config) {

	String sourceName = config.getSourceName();
	String dbName = config.getDatabase_name();
	String connIP = config.getDb_server_ip();
	String port = new Integer(config.getDb_server_port()).toString();
	String tableName = config.getRelationName();
	String username = config.getDb_username();
	String password = config.getDb_password();

	Connection c = Old_DBConnector.getOrCreateConnector(sourceName, DBType.POSTGRESQL, connIP, port, dbName, tableName,
		username, password);

	Old_DBConnector dbc = new Old_DBConnector(c, sourceName, DBType.POSTGRESQL, connIP, port, dbName, tableName, username,
		password);

	int id = ProfileTaskFactory.computeTaskId(sourceName, tableName);
	this.taskId = id;
	this.connector = dbc;
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
