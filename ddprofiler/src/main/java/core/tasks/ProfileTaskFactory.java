package core.tasks;

import core.config.sources.CSVSourceConfig;
import core.config.sources.PostgresSourceConfig;

public class ProfileTaskFactory {

    public static ProfileTask makeCSVProfileTask(CSVSourceConfig config) {

	ProfileTask pt = new CSVProfileTask(config);

	return pt;
    }

    public static ProfileTask makePostgresProfileTask(PostgresSourceConfig config) {

	ProfileTask pt = new PostgresProfileTask(config);

	return pt;
    }

    public static int computeTaskId(String path, String name) {
	String c = path.concat(name);
	return c.hashCode();
    }

}
