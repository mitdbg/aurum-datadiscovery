/**
 * @author Raul
 *
 */

package inputoutput.conn;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import inputoutput.Attribute;
import inputoutput.Record;
import inputoutput.TableInfo;

public class DBConnector extends Connector {

    private static final Logger log = Logger.getLogger(DBConnector.class.getName());

    private String dbName;
    private String connectPath;
    private String sourceName;

    // Open conn variables
    private boolean firstTime = true;
    private Statement theStatement;
    private ResultSet theRS;

    // Other connector variables
    private DBType db;// db system name e.g., mysq/oracle etc.
    private String username;// db conn user name;
    private String password; // db conn password;
    private String connIP;// for database
    private String port;// db connection port
    private Connection conn = null;
    private TableInfo tbInfo;
    private long currentOffset = 0;

    // Cache for connection pools
    private static Map<String, Connection> connectionPools = new HashMap<>();

    public static Connection getOrCreateConnector(String dbName, DBType dbType, String connIP, String port,
	    String connectPath, String filename, String username, String password) {

	// Definition of a conn identifier is here
	String connIdentifier = dbName + connIP + port;

	if (connectionPools.containsKey(connIdentifier)) {
	    return connectionPools.get(connIdentifier);
	}

	String cPath = null;

	try {
	    if (dbType == DBType.MYSQL) {
		Class.forName("com.mysql.jdbc.Driver");
		cPath = "jdbc:mysql://" + connIP + ":" + port + "/" + connectPath;
	    } else if (dbType == DBType.POSTGRESQL) {
		Class.forName("org.postgresql.Driver");
		cPath = "jdbc:postgresql://" + connIP + ":" + port + "/" + connectPath;
	    } else if (dbType == DBType.ORACLE) {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		cPath = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)" + "(HOST=" + connIP
			+ ")(PORT=" + port + ")))" + "(CONNECT_DATA=(SID=" + connectPath + ")))";
	    } else if (dbType == DBType.SQLSERVER) {
		cPath = String.format("jdbc:sqlserver://{}:{}; databaseName={};", connIP, port, dbName);
	    }
	} catch (ClassNotFoundException cnfe) {
	    cnfe.printStackTrace();
	    System.out.println(cnfe.getMessage());
	}

	// If no existing pool to handle this db, then we create a new one
	HikariConfig config = new HikariConfig();
	config.setJdbcUrl(cPath);
	config.setUsername(username);
	config.setPassword(password);
	config.addDataSourceProperty("cachePrepStmts", "true");
	config.addDataSourceProperty("prepStmtCacheSize", "250");
	config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
	config.addDataSourceProperty("maximumPoolSize", "1");
	HikariDataSource ds = new HikariDataSource(config);

	Connection connection = null;
	try {
	    connection = ds.getConnection();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	connectionPools.put(connIdentifier, connection);

	return connection;
    }

    public DBConnector() {
	this.tbInfo = new TableInfo();
    }

    public DBConnector(Connection conn, String dbName, DBType dbType, String connIP, String port, String connectPath,
	    String filename, String username, String password) {
	this.conn = conn;
	this.dbName = dbName;
	this.db = dbType;
	this.connIP = connIP;
	this.port = port;
	this.connectPath = connectPath;
	this.sourceName = filename;
	this.username = username;
	this.password = password;
	this.tbInfo = new TableInfo();

	// Initialize tbInfo
	List<Attribute> attrs = null;
	try {
	    attrs = this.getAttributes();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	this.tbInfo.setTableAttributes(attrs);
    }

    @Deprecated
    public DBConnector(String dbName, DBType dbType, String connIP, String port, String connectPath, String filename,
	    String username, String password) throws IOException {
	this.dbName = dbName;
	this.db = dbType;
	this.connIP = connIP;
	this.port = port;
	this.connectPath = connectPath;
	this.sourceName = filename;
	this.username = username;
	this.password = password;
	this.tbInfo = new TableInfo();
	this.initConnector();
    }

    @Deprecated
    @Override
    public void initConnector() throws IOException {
	String cPath = null;
	try {
	    if (db == DBType.MYSQL) {
		Class.forName("com.mysql.jdbc.Driver");
		cPath = "jdbc:mysql://" + connIP + ":" + port + "/" + connectPath;
		conn = DriverManager.getConnection(cPath, username, password);
	    } else if (db == DBType.POSTGRESQL) {
		Class.forName("org.postgresql.Driver");
		cPath = "jdbc:postgresql://" + connIP + ":" + port + "/" + connectPath;
		conn = DriverManager.getConnection(cPath, username, password);
	    } else if (db == DBType.ORACLE) {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		cPath = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)" + "(HOST=" + connIP
			+ ")(PORT=" + port + ")))" + "(CONNECT_DATA=(SID=" + connectPath + ")))";
		conn = DriverManager.getConnection(cPath, username, password);
	    }
	    this.connectPath = cPath;

	    List<Attribute> attrs = this.getAttributes();
	    this.tbInfo.setTableAttributes(attrs);
	} catch (ClassNotFoundException e) {
	    log.log(Level.SEVERE, "DB connection driver not found");
	    e.printStackTrace();
	} catch (SQLException e) {
	    log.log(Level.SEVERE, "Cannot connect to the database");
	    e.printStackTrace();
	}
    }

    private boolean handleFirstTime(int fetchSize) {
	String sql = "SELECT * FROM " + sourceName;

	try {
	    conn.setAutoCommit(false);
	    theStatement = conn.createStatement();
	    theStatement.setFetchSize(fetchSize);
	    theRS = theStatement.executeQuery(sql);
	} catch (SQLException sqle) {
	    System.out.println("ERROR: " + sqle.getLocalizedMessage());
	    return false;
	} catch (Exception e) {
	    System.out.println("ERROR: executeQuery failed");
	    return false;
	}
	return true;
    }

    @Override
    public boolean readRows(int num, List<Record> rec_list) throws IOException, SQLException {
	if (firstTime) {
	    handleFirstTime(num);
	    firstTime = false;
	}

	boolean new_row = false;

	while (num > 0 && theRS.next()) { // while there are some available and
					  // we need to read more records
	    new_row = true;

	    num--;
	    // FIXME: profile and optimize this
	    Record rec = new Record();
	    for (int i = 0; i < this.tbInfo.getTableAttributes().size(); i++) {
		Object obj = theRS.getObject(i + 1);
		if (obj != null) {
		    String v1 = obj.toString();
		    rec.getTuples().add(v1);
		} else {
		    rec.getTuples().add("");
		}
	    }
	    rec_list.add(rec);
	}

	return new_row;
    }

    @Override
    void destroyConnector() {
	// ...
    }

    @Override
    public List<Attribute> getAttributes() throws SQLException {
	if (tbInfo.getTableAttributes() != null)
	    return tbInfo.getTableAttributes();
	DatabaseMetaData metadata = conn.getMetaData();
	ResultSet resultSet = metadata.getColumns(null, null, sourceName, null);
	Vector<Attribute> attrs = new Vector<Attribute>();
	while (resultSet.next()) {
	    String name = resultSet.getString("COLUMN_NAME");
	    String type = resultSet.getString("TYPE_NAME");
	    int size = resultSet.getInt("COLUMN_SIZE");
	    Attribute attr = new Attribute(name, type, size);
	    attrs.addElement(attr);
	}
	resultSet.close();
	tbInfo.setTableAttributes(attrs);
	return attrs;
    }

    /*
     * setters and getters. This is a boring part, and could be ignored.
     */

    @Override
    public String getDBName() {
	return this.dbName;
    }

    @Override
    public String getPath() {
	return this.connectPath;
    }

    public void __setConnectPath(String connectPath) {
	this.connectPath = connectPath;
    }

    public String getFilename() {
	return this.sourceName;
    }

    public void setFilename(String filename) {
	this.sourceName = filename;
    }

    public DBType getDBType() {
	return db;
    }

    public void setDB(DBType db) {
	this.db = db;
    }

    public String getUsername() {
	return username;
    }

    public void setUsername(String username) {
	this.username = username;
    }

    public String getPassword() {
	return password;
    }

    public void setPassword(String password) {
	this.password = password;
    }

    public String getConnIP() {
	return connIP;
    }

    public void setConnIP(String connIP) {
	this.connIP = connIP;
    }

    public String getPort() {
	return port;
    }

    public void setPort(String port) {
	this.port = port;
    }

    @Override
    public String getSourceName() {
	return this.sourceName;
    }

    public void close() {
	// We have a pooled connection now so no need for this...
	try {
	    theRS.close();
	    theStatement.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }
}
