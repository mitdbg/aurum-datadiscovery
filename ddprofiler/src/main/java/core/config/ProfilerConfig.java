package core.config;

import java.util.List;
import java.util.Map;

import core.config.ConfigDef.Importance;
import core.config.ConfigDef.Type;

public class ProfilerConfig extends Config {

	private static final ConfigDef config;
	
	public static final String EXECUTION_MODE = "execution.mode";
	private static final String EXECUTION_MODE_DOC = "Online for server mode and Offline for one-shot";
	
	
	static{
		config = new ConfigDef()
				.define(EXECUTION_MODE, Type.STRING, "", Importance.HIGH, EXECUTION_MODE_DOC);
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

