package core;

import java.io.Closeable;
import java.io.IOException;

import inputoutput.conn.DBConnector;
import inputoutput.conn.DBType;
import inputoutput.conn.Connector;
import inputoutput.conn.FileConnector;

public class WorkerTask implements Closeable {

	private final int taskId;
	private Connector connector;
	
	private WorkerTask(int id, Connector connector) {
		this.taskId = id;
		this.connector = connector;
	}
	
	public int getTaskId() {
		return taskId;
	}
	
	public Connector getConnector() {
		return this.connector;
	}
	
	public static WorkerTask makeWorkerTaskForCSVFile(String path, String name, String separator) {
		FileConnector fc = null;
		try {
			fc = new FileConnector(path, name, separator);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		int id = computeTaskId(path, name);
		return new WorkerTask(id, fc);
	}
	
	public static WorkerTask makeWorkerTaskForDB(DBType dbType, String connIP, 
			String port, String sourceName, String tableName,
			String username, String password ) {
		DBConnector dbc = null;
		try{
			dbc = new DBConnector(dbType, connIP, port, sourceName, tableName, username, password);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		int id = computeTaskId(sourceName, tableName);
		return new WorkerTask(id, dbc);
	}
	
	private static int computeTaskId(String path, String name) {
		String c = path.concat(name);
		return c.hashCode();
	}

	@Override
	public void close() throws IOException {
		((DBConnector)connector).close();
	}
	
}
