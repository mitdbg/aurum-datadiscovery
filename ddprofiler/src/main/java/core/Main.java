/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package core;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import comm.WebServer;
import core.config.CommandLineArgs;
import core.config.ConfigKey;
import core.config.ProfilerConfig;
import inputoutput.conn.DBType;
import inputoutput.conn.DBUtils;
import joptsimple.OptionParser;
import store.Store;
import store.StoreFactory;

public class Main {
	
	final private Logger LOG = LoggerFactory.getLogger(Main.class.getName());
	
	public enum ExecutionMode {
		ONLINE(0),
		OFFLINE_FILES(1),
		OFFLINE_DB(2);
		
		int mode;
		
		ExecutionMode(int mode) {
			this.mode = mode;
		}
	}

	public void startProfiler(ProfilerConfig pc) {
		
		// Default is elastic, if we have more in the future, just pass a property to configure this
		Store s = StoreFactory.makeElasticStore(pc);
		
		//for test purpose, use this and comment above line when elasticsearch is not configured
		//Store s = StoreFactory.makeNullStore(pc);
		
		Conductor c = new Conductor(pc, s);
		c.start();
		
		int executionMode = pc.getInt(ProfilerConfig.EXECUTION_MODE);
		if(executionMode == ExecutionMode.ONLINE.mode) {
			// Start infrastructure for REST server
			WebServer ws = new WebServer(pc, c);
			ws.init();
		}
		else if (executionMode == ExecutionMode.OFFLINE_FILES.mode) {
			// Run with the configured input parameters and produce results to file (?)
			String pathToSources = pc.getString(ProfilerConfig.SOURCES_TO_ANALYZE_FOLDER);
			this.readDirectoryAndCreateTasks(c, pathToSources);
		}
		else if(executionMode == ExecutionMode.OFFLINE_DB.mode) {
			this.readTablesFromDBAndCreateTasks(c);
		}
		
		while(c.isTherePendingWork()) {
			List<WorkerTaskResult> results = null;
			do {
				results = c.consumeResults();
			} while(results.isEmpty());
			
			for(WorkerTaskResult wtr : results) {
				s.storeDocument(wtr);
			}
		}
	}
	
	public static void main(String args[]) {

		//try {
		//	Class.forName ("oracle.jdbc.driver.OracleDriver");
		//	Connection conn = DriverManager.getConnection(
		//			"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)"
		//			+ "(HOST=18.9.62.94)(PORT=1521)))"
		//			+ "(CONNECT_DATA=(SID=dwrhst)))", "datadisc", "rrdj1078");
		//	List<String> tables = DBUtils.getTablesFromDatabase(conn);
		//	for(String str : tables) {
		//		System.out.println(str);
		//	}
		//	
		//	Statement stat = conn.createStatement();
		//	String sql = " SELECT * FROM SUBJECT_SUMMARY LIMIT 10 OFFSET 0 ";
		//	sql = " SELECT * FROM (SELECT * row_number() over rnk FROM SUBJECT_SUMMARY) WHERE rnk BETWEEN 0 AND 10 ";
		//	sql = " SELECT * FROM ( SELECT * FROM SUBJECT_SUMMARY) WHERE ROWNUM BETWEEN 0 AND 10 ";
		//	ResultSet rs = stat.executeQuery(sql);
		//	ResultSetMetaData rsm = rs.getMetaData();
		//	System.out.println("COLUMNS: " +rsm.getColumnCount());
		//	int totalCol = rsm.getColumnCount();
		//	while(rs.next()){
		//		for(int i = 0; i<totalCol-1; i++) {
		//			Object obj = rs.getObject(i+1);
		//			if(obj != null) {
		//				String v1 = obj.toString();
		//				System.out.print(v1);
		//			}
		//			System.out.print(",");
		//		}
		//		System.out.println();
		//	}
		//	stat = conn.createStatement();
		//	sql = " select tablespace_name, table_name from all_tables ";
		//	//sql = " select tablespace_name, table_name from dba_tables ";
		//	rs = stat.executeQuery(sql);
		//	rsm = rs.getMetaData();
		//	System.out.println("COLUMNS: " +rsm.getColumnCount());
		//	totalCol = rsm.getColumnCount();
		//	while(rs.next()) {
		//		for(int i = 0; i < totalCol; i++) {
		//			Object obj = rs.getObject(i+1);
		//			if(obj != null) {
		//				String v1 = obj.toString();
		//				System.out.println(v1);
		//			}
		//			System.out.print(",");
		//		}
		//		System.out.println();
		//	}
		//	
		//} catch (SQLException e1) {
		//	e1.printStackTrace();
		//} catch (ClassNotFoundException e) {
		//	e.printStackTrace();
		//}
		//System.exit(0);
	//	

		// Get Properties with command line configuration
		List<ConfigKey> configKeys = ProfilerConfig.getAllConfigKey();
		OptionParser parser = new OptionParser();
		// Unrecognized options are passed through to the query
		parser.allowsUnrecognizedOptions();
		CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
		Properties commandLineProperties = cla.getProperties();
		
		// Check if the user requests help
		for (String a : args) {
			if(a.contains("help") || a.equals("?")) {
				try {
					parser.printHelpOn(System.out);
					System.exit(0);
				} 
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		// TODO: get properties from file ? 
		
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
			    	String path = filePath.getParent().toString()+File.separator;
			    	String name = filePath.getFileName().toString();
			    	String separator = ",";
			    	WorkerTask wt = WorkerTask.makeWorkerTaskForCSVFile(path, name, separator);
			    	c.submitTask(wt);
			    }
			});
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void readTablesFromDBAndCreateTasks(Conductor c) {
		Properties dbp = DBUtils.loadDBPropertiesFromFile();
		String dbTypeStr = dbp.getProperty("db_system_name");
		DBType dbType = getType(dbTypeStr);
		
		String ip = dbp.getProperty("conn_ip");
		String port = dbp.getProperty("port");
		String dbname = dbp.getProperty("conn_path");
		String username = dbp.getProperty("user_name");
		String password = dbp.getProperty("password");
		
		LOG.info("Conn to DB on: {}:{}/{}", ip, port, dbname);
		
		Connection dbConn = DBUtils.getDBConnection(dbType, ip, port, dbname, username, password);
		
		List<String> tables = DBUtils.getTablesFromDatabase(dbConn);
		for(String str : tables) {
			LOG.info("Detected relational table: {}", str);
			WorkerTask wt = WorkerTask.makeWorkerTaskForDB(dbType, ip, port, dbname, str, username, password);
			c.submitTask(wt);
		}
	}
	
	private DBType getType(String type) {
		if(type.equals("mysql")) return DBType.MYSQL;
		else if(type.equals("postgresql")) return DBType.POSTGRESQL;
		else if(type.equals("oracle")) return DBType.ORACLE;
		else return null;
	}
	
	public static Properties validateProperties(Properties p) {
		// TODO: Go over all properties configured here and validate their ranges, values
		// etc. Stop the program and spit useful doc message when something goes wrong.
		// Return the unmodified properties if everything goes well.
		
		return p;
	}
	
}
