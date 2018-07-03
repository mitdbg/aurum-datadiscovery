package sources.tasks;

import sources.config.CSVSourceConfig;
import sources.config.HiveSourceConfig;
import sources.config.PostgresSourceConfig;
import sources.config.SQLServerSourceConfig;

public class ProfileTaskFactory {

    public static ProfileTask makeCSVProfileTask(CSVSourceConfig config) {
	ProfileTask pt = new CSVProfileTask(config);
	return pt;
    }

    public static ProfileTask makePostgresProfileTask(PostgresSourceConfig config) {
	ProfileTask pt = new PostgresProfileTask(config);
	return pt;
    }

    public static ProfileTask makeSQLServerProfileTask(SQLServerSourceConfig config) {
	ProfileTask pt = new SQLServerProfileTask(config);
	return pt;
    }

    public static ProfileTask makeHiveProfileTask(HiveSourceConfig config) {
	ProfileTask pt = new HiveProfileTask(config);
	return pt;
    }

    public static int computeTaskId(String path, String name) {
	String c = path.concat(name);
	return c.hashCode();
    }

}
