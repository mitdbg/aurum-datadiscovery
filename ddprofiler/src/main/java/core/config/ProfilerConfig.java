/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package core.config;

import java.util.List;
import java.util.Map;

import core.config.ConfigDef.Importance;
import core.config.ConfigDef.Type;

public class ProfilerConfig extends Config {

	private static final ConfigDef config;
	
	public static final String EXECUTION_MODE = "execution.mode";
	private static final String EXECUTION_MODE_DOC = "(online) for server mode and (offline) for one-shot";

	public static final String WEB_SERVER_PORT = "web.server.port";
	private static final String WEB_SERVER_PORT_DOC = "The port where web server listens";

	public static final String NUM_POOL_THREADS = "num.pool.threads";
	private static final String NUM_POOL_THREADS_DOC = "Number of threads in the worker pool";

	public static final String NUM_RECORD_READ = "num.record.read";
	private static final String NUM_RECORD_READ_DOC = "Number of records to read per interaction with the data sources";
	
	
	static{
		config = new ConfigDef()
				.define(EXECUTION_MODE, Type.INT, 0, Importance.HIGH, EXECUTION_MODE_DOC)
				.define(WEB_SERVER_PORT, Type.INT, 8080, Importance.MEDIUM, WEB_SERVER_PORT_DOC)
				.define(NUM_POOL_THREADS, Type.INT, 4, Importance.LOW, NUM_POOL_THREADS_DOC)
				.define(NUM_RECORD_READ, Type.INT, 1000, Importance.MEDIUM, NUM_RECORD_READ_DOC); 
	}
	
	public ProfilerConfig(Map<? extends Object, ? extends Object> originals) {
		super(config, originals);
	}
	
	public static ConfigKey getConfigKey(String name){
		return config.getConfigKey(name);
	}
	
	public static List<ConfigKey> getAllConfigKey(){
		return config.getAllConfigKey();
	}
	
	public static void main(String[] args) {
        System.out.println(config.toHtmlTable());
    }

}

