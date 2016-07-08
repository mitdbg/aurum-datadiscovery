/**
 * @author Sibo Wang
 * @author Raul (edits)
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
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import inputoutput.Attribute;
import inputoutput.Record;
import inputoutput.TableInfo;

public class DBConnector extends Connector {
	
	private static final Logger log = Logger.getLogger(DBConnector.class.getName());

	private DBType db;// db system name e.g., mysq/oracle etc.
	private String username;// db conn user name;
	private String password; // db conn password;
	private String connIP;// for database
	private String port;// db connection port
	private Connection conn = null;
	private TableInfo tbInfo;
	private Statement stat;	
	private long currentOffset = 0;
	
	public DBConnector() {
		this.tbInfo = new TableInfo();
	}
	
	public DBConnector(DBType dbType, String connIP, String port,
			String connectPath, String filename, String username, String password) throws IOException{
		this.db = dbType;
		this.connIP = connIP;
		this.port = port;
		this.connectPath = connectPath;
		this.sourceName = filename;
		this.username =username;
		this.password = password;
		this.tbInfo = new TableInfo();
		this.initConnector();
	}

	@Override
	public void initConnector() throws IOException {
		try {
			if(db == DBType.MYSQL) {
				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection("jdbc:mysql://" + 
						connIP + ":" + port + "/" + connectPath, username, password);
			}
			else if(db == DBType.POSTGRESQL) {
				Class.forName("org.postgresql.Driver");
				conn = DriverManager.getConnection("jdbc:postgresql://" + 
						connIP + ":" + port + "/" + connectPath, username, password);
			}
			else if(db == DBType.ORACLE) {
				Class.forName ("oracle.jdbc.driver.OracleDriver");
				conn = DriverManager.getConnection(
						"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)"
						+ "(HOST="+connIP+")(PORT="+port+")))"
						+ "(CONNECT_DATA=(SID="+connectPath+")))", username, password);
			}

			List<Attribute> attrs = this.getAttributes();
			this.tbInfo.setTableAttributes(attrs);
		} 
		catch (ClassNotFoundException e) {
			log.log(Level.SEVERE, "DB connection driver not found");
			e.printStackTrace();
		} 
		catch (SQLException e) {
			log.log(Level.SEVERE, "Cannot connect to the database");
			e.printStackTrace();
		}
	}

	@Override
	public boolean readRows(int num, List<Record> rec_list) throws IOException, SQLException {
		stat = conn.createStatement();
		String sql = null;
		if(this.db == DBType.POSTGRESQL) {
			sql = "SELECT * FROM "+sourceName+ " LIMIT "+ num + " OFFSET " + currentOffset;
		}
		else if(this.db == DBType.ORACLE) {
			sql = " SELECT * FROM ( SELECT * FROM "+sourceName+") WHERE ROWNUM BETWEEN "+num+" AND "+currentOffset+" ";
		}

		ResultSet rs = stat.executeQuery(sql);
		boolean new_row = false;
		while(rs.next()) {
			new_row = true;
			Record rec = new Record();
			for(int i = 0; i < this.tbInfo.getTableAttributes().size(); i++) {
				String v1 = rs.getObject(i+1).toString();
				rec.getTuples().add(v1);
			}
			rec_list.add(rec);
		}
		currentOffset += rec_list.size();
		rs.close();
		return new_row;
	}

	@Override
	void destroyConnector() {
		try {
			conn.close();
			stat.close();
		} 
		catch (SQLException e) {
			log.log(Level.SEVERE, "Cannot close the connection to the database");
			e.printStackTrace();
		}
	}

	@Override
	public List<Attribute> getAttributes() throws SQLException {
		if(tbInfo.getTableAttributes() != null)
			return tbInfo.getTableAttributes();
		DatabaseMetaData metadata = conn.getMetaData();
		ResultSet resultSet = metadata.getColumns(null, null, sourceName, null);
		Vector<Attribute> attrs  = new Vector<Attribute>();
		while (resultSet.next()) {
			String name = resultSet.getString("COLUMN_NAME");
			String type = resultSet.getString("TYPE_NAME");
			int size = resultSet.getInt("COLUMN_SIZE");
			Attribute attr = new Attribute(name, type, size);
			attrs.addElement(attr);
		}
		tbInfo.setTableAttributes(attrs);
		return attrs;
	}
	
/*
 * setters and getters. This is a boring part, and could be ignored.	
 */
	
	public String getConnectPath(){
		return this.connectPath;
	}
	public void setConnectPath(String connectPath){
		this.connectPath = connectPath;
	}
	
	public String getFilename(){
		return this.sourceName;
	}
	
	public void setFilename(String filename){
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

	public Statement getStat() {
		return stat;
	}

	public void setStat(Statement stat) {
		this.stat = stat;
	}

	@Override
	public String getSourceName() {
		return this.conn.toString();
	}

	public void close() {
		try {
			conn.close();
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
}