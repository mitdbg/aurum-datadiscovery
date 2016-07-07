package inputoutput.conn;

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

public class DBUtils {

	public static List<String> getTablesFromDatabase(Connection conn) {
		List<String> tables = new ArrayList<>();
		String types[] = new String[]{"TABLE", "VIEW"}; 
		
		DatabaseMetaData md;
		try {
			md = conn.getMetaData();
			ResultSet rs = md.getTables(null, null, "%", types);
			while (rs.next()) {
				String tn = rs.getString(3);
				tables.add(tn);
			}
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		return tables;
	}
	
	public static Properties loadDBPropertiesFromFile() {
		Properties prop = new Properties();
		try {
			InputStream is = DBUtils.class.getClassLoader()
					.getResource("dbconnector.config").openStream();
			prop.load(is);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		return prop;
	}
	
	public static Connection getMYSQLConnection(String connIP, 
			String port, String dbName, String username, String password) {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://" + 
					connIP + ":" + port + "/" + dbName, username, password);
		} 
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		return conn;
	}
	
	public static Connection getPOSTGRESQLConnection(String connIP, 
			String port, String dbName, String username, String password) {
		Connection conn = null;
		try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection("jdbc:postgresql://" + 
					connIP + ":" + port + "/" + dbName, username, password);
		} 
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		return conn;
	}
	
	public static Connection getOracle10GConnection(String connIP, 
			String port, String dbName, String username, String password) {
		Connection conn = null;
		try {
			Class.forName ("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection
			        ("jdbc:oracle:thin:@//"+connIP+":"+port+"/"+dbName+"", 
			        		username, password);
		} 
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		return conn;
	}
	
}
