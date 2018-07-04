package sources.tasks;

import sources.config.CSVSourceConfig;
import sources.config.HiveSourceConfig;
import sources.config.PostgresSourceConfig;
import sources.config.SQLServerSourceConfig;

public class ProfileTaskFactory {

    public static ProfileTask_old makeCSVProfileTask(CSVSourceConfig config) {
	ProfileTask_old pt = new CSVProfileTask(config);
	return pt;
    }

    public static ProfileTask_old makePostgresProfileTask(PostgresSourceConfig config) {
	ProfileTask_old pt = new PostgresProfileTask(config);
	return pt;
    }

    public static ProfileTask_old makeSQLServerProfileTask(SQLServerSourceConfig config) {
	ProfileTask_old pt = new SQLServerProfileTask(config);
	return pt;
    }

    public static ProfileTask_old makeHiveProfileTask(HiveSourceConfig config) {
	ProfileTask_old pt = new HiveProfileTask(config);
	return pt;
    }

    public static int computeTaskId(String path, String name) {
	String c = path.concat(name);
	return c.hashCode();
    }

}
