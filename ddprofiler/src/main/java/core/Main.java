/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import comm.WebServer;
import core.config.CommandLineArgs;
import core.config.ConfigKey;
import core.config.ProfilerConfig;
import joptsimple.OptionParser;
import store.Store;
import store.StoreFactory;

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
		
		// Default is elastic, if we have more in the future, just pass a property to configure this
		Store s = StoreFactory.makeElasticStore(pc);
		
		Conductor c = new Conductor(pc, s);
		c.start();
		
		int executionMode = pc.getInt(ProfilerConfig.EXECUTION_MODE);
		if(executionMode == ExecutionMode.ONLINE.mode) {
			// Start infrastructure for REST server
			WebServer ws = new WebServer(pc, c);
			ws.init();
		}
		else if (executionMode == ExecutionMode.OFFLINE.mode) {
			// Run with the configured input parameters and produce results to file (?)
			String pathToSources = pc.getString(ProfilerConfig.SOURCES_TO_ANALYZE_FOLDER);
			this.readDirectoryAndCreateTasks(c, pathToSources);
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
	
	private void readDirectoryAndCreateTasks(Conductor c, String pathToSources) {
		try {
			Files.walk(Paths.get(pathToSources)).forEach(filePath -> {
			    if (Files.isRegularFile(filePath)) {
			    	String path = null; // TODO:
			    	String name = null; // TODO:
			    	String separator = null; // TODO:
			    	WorkerTask wt = WorkerTask.makeWorkerTaskForCSVFile(path, name, separator);
			    	c.submitTask(wt);
			    }
			});
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static Properties validateProperties(Properties p) {
		// TODO: Go over all properties configured here and validate their ranges, values
		// etc. Stop the program and spit useful doc message when something goes wrong.
		// Return the unmodified properties if everything goes well.
		
		return p;
	}
	
}
