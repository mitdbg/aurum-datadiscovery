package inputoutput.connectors;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.Main;

public class DBUtils {

    final static private Logger LOG = LoggerFactory.getLogger(Main.class.getName());

    public static List<String> getTablesFromDatabase(Connection conn, String dbschema) {

	List<String> tables = new ArrayList<>();
	String types[] = new String[] { "TABLE", "VIEW" };

	DatabaseMetaData md;
	try {
	    md = conn.getMetaData();
	    ResultSet rs;
	    if ("".equals(dbschema) || "default".equals(dbschema))
		rs = md.getTables(null, null, "%", types);
	    else
		rs = md.getTables(null, dbschema, "%", types);
	    while (rs.next()) {
		String tn = rs.getString(3);
		tables.add(tn);
	    }
	} catch (SQLException e) {
	    e.printStackTrace();
	}

	return tables;
    }

    @Deprecated
    public static Properties loadDBPropertiesFromFile() {
	Properties prop = new Properties();
	try {
	    InputStream is = DBUtils.class.getClassLoader().getResource("dbconnector.config").openStream();
	    prop.load(is);
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return prop;
    }

    public static Connection getDBConnection(DBType type, String connIP, String port, String dbName, String username,
	    String password) {
	Connection conn = null;
	if (type == DBType.MYSQL) {
	    conn = getMYSQLConnection(connIP, port, dbName, username, password);
	} else if (type == DBType.POSTGRESQL) {
	    conn = getPOSTGRESQLConnection(connIP, port, dbName, username, password);
	} else if (type == DBType.ORACLE) {
	    conn = getOracle10GConnection(connIP, port, dbName, username, password);
	} else if (type == DBType.SQLSERVER) {
	    conn = getSQLServerConnection(connIP, port, dbName, username, password);
	}
	return conn;
    }

    private static Connection getMYSQLConnection(String connIP, String port, String dbName, String username,
	    String password) {
	Connection conn = null;
	try {
	    Class.forName("com.mysql.jdbc.Driver");
	    conn = DriverManager.getConnection("jdbc:mysql://" + connIP + ":" + port + "/" + dbName, username,
		    password);
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	} catch (SQLException e) {
	    e.printStackTrace();
	}

	return conn;
    }

    private static Connection getPOSTGRESQLConnection(String connIP, String port, String dbName, String username,
	    String password) {
	Connection conn = null;
	try {
	    Class.forName("org.postgresql.Driver");
	    conn = DriverManager.getConnection("jdbc:postgresql://" + connIP + ":" + port + "/" + dbName, username,
		    password);
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	} catch (SQLException e) {
	    e.printStackTrace();
	}

	return conn;
    }

    private static Connection getOracle10GConnection(String connIP, String port, String dbName, String username,
	    String password) {
	Connection conn = null;
	try {
	    Class.forName("oracle.jdbc.driver.OracleDriver");
	    conn = DriverManager
		    .getConnection(
			    "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)" + "(HOST=" + connIP
				    + ")(PORT=" + port + ")))" + "(CONNECT_DATA=(SID=" + dbName + ")))",
			    username, password);
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	} catch (SQLException e) {
	    e.printStackTrace();
	}

	return conn;
    }

    private static Connection getSQLServerConnection(String connIP, String port, String dbName, String username,
	    String password) {
	Connection conn = null;
	try {
	    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	    String connString = "jdbc:sqlserver://" + connIP + ":" + port + "; " + "databaseName=" + dbName + "; user="
		    + username + "; password=" + password + ";";
	    LOG.info("SQLServer conn string: {}", connString);
	    conn = DriverManager.getConnection(connString);
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	} catch (SQLException e) {
	    e.printStackTrace();
	}

	return conn;
    }
}
