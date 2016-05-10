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
	
	
	static{
		config = new ConfigDef()
				.define(EXECUTION_MODE, Type.INT, 0, Importance.HIGH, EXECUTION_MODE_DOC)
				.define(WEB_SERVER_PORT, Type.INT, 8080, Importance.MEDIUM, WEB_SERVER_PORT_DOC);
				
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

