package core.tasks;

import java.sql.Connection;

import core.SourceType;
import core.config.sources.SQLServerSourceConfig;
import inputoutput.connectors.DBType;
import inputoutput.connectors.Old_Connector;
import inputoutput.connectors.Old_DBConnector;

public class SQLServerProfileTask implements ProfileTask {

    private int taskId;
    private Old_Connector connector;

    public SQLServerProfileTask(SQLServerSourceConfig config) {
	String sourceName = config.getSourceName();
	String dbName = config.getDatabase_name();
	String connIP = config.getDb_server_ip();
	String port = new Integer(config.getDb_server_port()).toString();
	String tableName = config.getRelationName();
	String username = config.getDb_username();
	String password = config.getDb_password();

	Connection c = Old_DBConnector.getOrCreateConnector(sourceName, DBType.SQLSERVER, connIP, port, dbName, tableName,
		username, password);

	Old_DBConnector dbc = new Old_DBConnector(c, sourceName, DBType.SQLSERVER, connIP, port, dbName, tableName, username,
		password);

	int id = ProfileTaskFactory.computeTaskId(sourceName, tableName);
	this.taskId = id;
	this.connector = dbc;
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
    public SourceType getSourceType() {
	return SourceType.sqlserver;
    }

    @Override
    public void close() {
	this.connector.close();
    }

}
