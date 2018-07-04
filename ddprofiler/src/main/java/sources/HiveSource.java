package sources;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.Conductor;
import sources.config.HiveSourceConfig;
import sources.config.SourceConfig;
import sources.main.DBUtils;
import sources.main.SourceType;
import sources.tasks.ProfileTask_old;
import sources.tasks.ProfileTaskFactory;

public class HiveSource implements Source_old {

    final private Logger LOG = LoggerFactory.getLogger(HiveSource.class.getName());

    @Override
    public void processSource(SourceConfig config, Conductor c) {
	assert (config instanceof HiveSourceConfig);

	HiveSourceConfig hiveConfig = (HiveSourceConfig) config;

	// TODO: at this point we'll be harnessing metadata from the source

	String ip = hiveConfig.getHive_server_ip();
	String port = new Integer(hiveConfig.getHive_server_port()).toString();
	String dbName = hiveConfig.getDatabase_name();

	LOG.info("Conn to Hive on: {}:{}", ip, port);

	// FIXME: remove this enum; simplify this
	Connection hiveConn = DBUtils.getDBConnection(SourceType.hive, ip, port, dbName, null, null);

	List<String> tables = DBUtils.getTablesFromDatabase(hiveConn, null);
	try {
	    hiveConn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	for (String relation : tables) {
	    LOG.info("Detected relational table: {}", relation);

	    HiveSourceConfig relationHiveSourceConfig = (HiveSourceConfig) hiveConfig.selfCopy();
	    relationHiveSourceConfig.setRelationName(relation);

	    ProfileTask_old pt = ProfileTaskFactory.makeHiveProfileTask(relationHiveSourceConfig);

	    c.submitTask(pt);
	}

    }

}
