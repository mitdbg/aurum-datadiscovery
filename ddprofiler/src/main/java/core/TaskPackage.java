package core;

import inputoutput.conn.DBType;

public class TaskPackage {

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

  private TaskPackageType type;

  public enum TaskPackageType { CSV, DB }

  private TaskPackage(String dbName, String path, String name, String separator,
                      TaskPackageType type) {
	this.dbName = dbName;
    this.path = path;
    this.name = name;
    this.separator = separator;
    this.type = type;
  }

  private TaskPackage(String dbName, DBType dbType, String ip, String port, String dbname,
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
  }

  public static TaskPackage makeCSVFileTaskPackage(String dbName, String path, String name,
                                                   String separator) {
    return new TaskPackage(dbName, path, name, separator, TaskPackageType.CSV);
  }

  public static TaskPackage makeDBTaskPackage(String dbName, DBType dbType, String ip,
                                              String port, String dbname,
                                              String str, String username,
                                              String password) {
    return new TaskPackage(dbName, dbType, ip, port, dbname, str, username, password,
                           TaskPackageType.DB);
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
}
