package core.tasks;

import java.sql.Connection;

import core.SourceType;
import core.config.sources.SQLServerSourceConfig;
import inputoutput.conn.Connector;
import inputoutput.conn.DBConnector;
import inputoutput.conn.DBType;

public class SQLServerProfileTask implements ProfileTask {

    private int taskId;
    private Connector connector;

    public SQLServerProfileTask(SQLServerSourceConfig config) {
	String sourceName = config.getSourceName();
	String dbName = config.getDatabase_name();
	String connIP = config.getDb_server_ip();
	String port = new Integer(config.getDb_server_port()).toString();
	String tableName = config.getRelationName();
	String username = config.getDb_username();
	String password = config.getDb_password();

	Connection c = DBConnector.getOrCreateConnector(sourceName, DBType.SQLSERVER, connIP, port, dbName, tableName,
		username, password);

	DBConnector dbc = new DBConnector(c, sourceName, DBType.SQLSERVER, connIP, port, dbName, tableName, username,
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
    public Connector getConnector() {
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
