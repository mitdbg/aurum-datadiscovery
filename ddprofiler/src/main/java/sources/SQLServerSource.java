package sources;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.Conductor;
import core.config.sources.SQLServerSourceConfig;
import core.config.sources.SourceConfig;
import core.tasks.ProfileTask;
import core.tasks.ProfileTaskFactory;
import inputoutput.connectors.DBType;
import inputoutput.connectors.DBUtils;

public class SQLServerSource implements Source {

    final private Logger LOG = LoggerFactory.getLogger(SQLServerSource.class.getName());

    @Override
    public void processSource(SourceConfig config, Conductor c) {
	assert (config instanceof SQLServerSourceConfig);

	SQLServerSourceConfig sqlServerConfig = (SQLServerSourceConfig) config;

	// TODO: at this point we'll be harnessing metadata from the source

	String ip = sqlServerConfig.getDb_server_ip();
	String port = new Integer(sqlServerConfig.getDb_server_port()).toString();
	String db_name = sqlServerConfig.getDatabase_name();
	String username = sqlServerConfig.getDb_username();
	String password = sqlServerConfig.getDb_password();
	String dbschema = "default";

	LOG.info("Conn to DB on: {}:{}/{}", ip, port, db_name);

	// FIXME: remove this enum; simplify this
	Connection dbConn = DBUtils.getDBConnection(DBType.SQLSERVER, ip, port, db_name, username, password);

	List<String> tables = DBUtils.getTablesFromDatabase(dbConn, dbschema);
	try {
	    dbConn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	for (String relation : tables) {
	    LOG.info("Detected relational table: {}", relation);

	    SQLServerSourceConfig relationSQLServerSourceConfig = (SQLServerSourceConfig) sqlServerConfig.selfCopy();
	    relationSQLServerSourceConfig.setRelationName(relation);

	    ProfileTask pt = ProfileTaskFactory.makeSQLServerProfileTask(relationSQLServerSourceConfig);

	    c.submitTask(pt);
	}

    }

}
