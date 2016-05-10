package core;

import java.util.List;
import java.util.Properties;

import comm.WebServer;
import core.config.CommandLineArgs;
import core.config.ConfigKey;
import core.config.ProfilerConfig;
import joptsimple.OptionParser;

public class Main {
	
	public enum ExecutionMode {
		ONLINE(0),
		OFFLINE(0);
		
		int mode;
		
		ExecutionMode(int mode) {
			this.mode = mode;
		}
	}

	public void startProfiler(ProfilerConfig pc) {
		int executionMode = pc.getInt(ProfilerConfig.EXECUTION_MODE);
		if(executionMode == ExecutionMode.ONLINE.mode) {
			// Start infrastructure for REST server
			WebServer ws = new WebServer(pc);
			ws.init();
		}
		else if (executionMode == ExecutionMode.OFFLINE.mode) {
			// Run with the configured input parameters and produce results to file (?)
		}
	}
	
	public static void main(String args[]) {
		
		// Get Properties with command line configuration 
		List<ConfigKey> configKeys = ProfilerConfig.getAllConfigKey();
		OptionParser parser = new OptionParser();
		// Unrecognized options are passed through to the query
		parser.allowsUnrecognizedOptions();
		CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
		Properties commandLineProperties = cla.getProperties();
		
		// TODO: Get additional properties defined in files, etc
		
		// TODO: Merge all properties into one single Properties object to be validated
		// Pay attention to redefinition of properties and define a priority to fix
		// conflicts.
		
		Properties validatedProperties = validateProperties(commandLineProperties);
		
		ProfilerConfig pc = new ProfilerConfig(validatedProperties);
		
		// Start main
		
		Main m = new Main();
		m.startProfiler(pc);
	}
	
	private static Properties validateProperties(Properties p) {
		// TODO: Go over all properties configured here and validate their ranges, values
		// etc. Stop the program and spit useful doc message when something goes wrong.
		// Return the unmodified properties if everything goes well.
		
		return p;
	}
	
}
