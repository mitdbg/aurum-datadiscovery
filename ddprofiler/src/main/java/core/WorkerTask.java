package core;

import java.io.Closeable;
import java.io.IOException;

import inputoutput.conn.Connector;
import inputoutput.conn.DBConnector;
import inputoutput.conn.DBType;
import inputoutput.conn.FileConnector;

public class WorkerTask implements Closeable {

  private String dbName;

  // CSV properties
  private String path;
  private String name;
  private String separator;

  // DB properties
  private DBType dbType;
  private String ip;
  private String port;
  private String str;
  private String username;
  private String password;

  // For reading values
  private Connector connector;

  // Static global for benchmarking purposes

  private TaskPackageType type;

  public enum TaskPackageType { CSV, DB, BENCH }

  private WorkerTask(String dbName, String path, String name, String separator,
                     TaskPackageType type) {
	this.dbName = dbName;
    this.path = path;
    this.name = name;
    this.separator = separator;
    this.type = type;
    try {
      this.connector = new FileConnector(dbName, path, name, separator);
    } catch (IOException e) {
      this.connector = null;
      e.printStackTrace();
    }
  }

  private WorkerTask(String dbName, DBType dbType, String ip, String port, String dbname,
                     String str, String username, String password,
                     TaskPackageType type) {
    this.dbType = dbType;
    this.ip = ip;
    this.port = port;
    this.dbName = dbName;
    this.str = str;
    this.username = username;
    this.password = password;
    this.type = type;
    try {
      this.connector = new DBConnector(dbName, dbType, ip, port, dbName, str, username, password);
    } catch (IOException e) {
      this.connector = null;
      e.printStackTrace();
    }
  }
  
  private WorkerTask(String path, String separator) {
    this.path = path;
    this.separator = separator;
    this.type = TaskPackageType.BENCH;
  }

  public static WorkerTask makeCSVFileWorkerTask(String dbName, String path, String name,
                                                 String separator) {
    return new WorkerTask(dbName, path, name, separator, TaskPackageType.CSV);
  }

  public static WorkerTask makeDBWorkerTask(String dbName, DBType dbType, String ip,
                                            String port, String dbname,
                                            String str, String username,
                                            String password) {
    return new WorkerTask(dbName, dbType, ip, port, dbname, str, username, password,
                           TaskPackageType.DB);
  }
 
  public static WorkerTask makeBenchmarkTask(String path, String separator) {
		return new WorkerTask(path, separator);
  }

  public int getID(int index) {
    String source = String.valueOf(index);
    if (this.type.equals(TaskPackageType.CSV)) {
      source.concat(path).concat(name);
    } else if (this.type.equals(TaskPackageType.CSV)) {
      source.concat(dbName).concat(str);
    }
    return source.hashCode();
  }

  public String getPath() { return path; }

  public String getName() { return name; }

  public String getSeparator() { return separator; }

  public TaskPackageType getType() { return type; }

  public DBType getDBType() { return dbType; }

  public String getIp() { return ip; }

  public String getPort() { return port; }

  public String getDBName() { return dbName; }

  public String getStr() { return str; }

  public String getUsername() { return username; }

  public String getPassword() { return password; }

  public Connector getConnector() { return connector; }

  @Override
  public void close() throws IOException {
    connector.close();
  }
}
