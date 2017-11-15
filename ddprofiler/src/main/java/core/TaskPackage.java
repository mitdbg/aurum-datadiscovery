package core;

import inputoutput.conn.DBType;

public class TaskPackage {

    private String sourceName;

    // CSV properties
    private String path;
    private org.apache.hadoop.fs.Path hdfs_path;
    private String fileName;
    private String separator;

    // DB properties
    private DBType dbType;
    private String dbName;
    private String ip;
    private String port;
    private String str;
    private String username;
    private String password;

    // Static global for benchmarking purposes

    private TaskPackageType type;

    public enum TaskPackageType {
	CSV, DB, BENCH, HDFSCSV
    }

    private TaskPackage(String sourceName, String path, String fileName, String separator, TaskPackageType type) {
	this.sourceName = sourceName;
	this.path = path;
	this.fileName = fileName;
	this.separator = separator;
	this.type = type;
    }

    private TaskPackage(String sourceName, org.apache.hadoop.fs.Path path, String fileName, String separator,
	    TaskPackageType type) {
	this.sourceName = sourceName;
	this.hdfs_path = path;
	this.fileName = fileName;
	this.separator = separator;
	this.type = type;
    }

    private TaskPackage(String sourceName, DBType dbType, String ip, String port, String dbName, String str,
	    String username, String password, TaskPackageType type) {
	this.sourceName = sourceName;
	this.dbType = dbType;
	this.ip = ip;
	this.port = port;
	this.dbName = dbName;
	this.str = str;
	this.username = username;
	this.password = password;
	this.type = type;
    }

    private TaskPackage(String path, String separator) {
	this.path = path;
	this.separator = separator;
	this.type = TaskPackageType.BENCH;
    }

    public static TaskPackage makeCSVFileTaskPackage(String dbName, String path, String name, String separator) {
	return new TaskPackage(dbName, path, name, separator, TaskPackageType.CSV);
    }

    public static TaskPackage makeDBTaskPackage(String sourceName, DBType dbType, String ip, String port, String dbname,
	    String str, String username, String password) {
	return new TaskPackage(sourceName, dbType, ip, port, dbname, str, username, password, TaskPackageType.DB);
    }

    public static TaskPackage makeBenchmarkTask(String path, String separator) {
	return new TaskPackage(path, separator);
    }

    public String getSourceName() {
	return sourceName;
    }

    public String getPath() {
	return path;
    }

    public org.apache.hadoop.fs.Path getHdfsPath() {
	return hdfs_path;
    }

    public String getFileName() {
	return fileName;
    }

    public String getSeparator() {
	return separator;
    }

    public TaskPackageType getType() {
	return type;
    }

    public DBType getDBType() {
	return dbType;
    }

    public String getIp() {
	return ip;
    }

    public String getPort() {
	return port;
    }

    public String getDBName() {
	return dbName;
    }

    public String getStr() {
	return str;
    }

    public String getUsername() {
	return username;
    }

    public String getPassword() {
	return password;
    }

    public static TaskPackage makeHDFSCSVFileTaskPackage(String dbName, org.apache.hadoop.fs.Path toFile, String name,
	    String separator) {
	return new TaskPackage(dbName, toFile, name, separator, TaskPackageType.HDFSCSV);
    }

}
