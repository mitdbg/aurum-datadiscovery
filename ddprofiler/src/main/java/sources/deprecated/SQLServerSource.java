package sources.deprecated;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.Conductor;
import sources.SourceType;
import sources.SourceUtils;
import sources.config.SQLServerSourceConfig;
import sources.config.SourceConfig;

public class SQLServerSource implements Source_old {

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
	Connection dbConn = SourceUtils.getDBConnection(SourceType.sqlserver, ip, port, db_name, username, password);

	List<String> tables = SourceUtils.getTablesFromDatabase(dbConn, dbschema);
	try {
	    dbConn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	for (String relation : tables) {
	    LOG.info("Detected relational table: {}", relation);

	    SQLServerSourceConfig relationSQLServerSourceConfig = (SQLServerSourceConfig) sqlServerConfig.selfCopy();
	    relationSQLServerSourceConfig.setRelationName(relation);

	    ProfileTask_old pt = ProfileTaskFactory.makeSQLServerProfileTask(relationSQLServerSourceConfig);

	    c.submitTask(pt);
	}

    }

}
