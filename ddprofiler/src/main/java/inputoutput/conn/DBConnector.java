/**
 * @author Sibo Wang
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
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import inputoutput.Attribute;
import inputoutput.Record;
import inputoutput.TableInfo;

public class DBConnector extends Connector {

	private static final Logger log = Logger.getLogger(DBConnector.class.getName());

	private String db;// db system name e.g., mysq/oracle etc.
	private String username;// db conn user name;
	private String password; // db conn password;
	private String connIP;// for database
	private String port;// db connection port
	Connection conn = null;
	private TableInfo tbInfo;
	private Statement stat;	
	private long currOffset = 0;
	
	public DBConnector() {
		this.tbInfo = new TableInfo();
	}
	
	public DBConnector(String db, String connIP, String port,
			String connectPath, String filename, String username, String password) throws IOException{
		this.db = db;
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
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:" + db + "://" + connIP + ":" + port + connectPath,
					username, password);

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

	@Override
	public boolean readRows(int num, List<Record> rec_list) throws IOException, SQLException {
		stat = conn.createStatement();
		String sql = "select * from "+sourceName+ " LIMIT "+ currOffset+","+num;
		//String sql = "select * from "+filename+ " LIMIT "+ num;

		//System.out.println(sql);
		ResultSet rs = stat.executeQuery(sql);
		boolean new_row = false;
		while(rs.next()){
			new_row = true;
			Record rec = new Record();
			for(int i=0; i<this.tbInfo.getTableAttributes().size(); i++){
				String v1 = rs.getObject(i+1).toString();
				//System.out.println(v1);
				rec.getTuples().add(v1);
			}
			rec_list.add(rec);
			//System.out.println(rec);
		}
		currOffset+=rec_list.size();
		rs.close();
		return new_row;
	}

	@Override
	void destroyConnector() {
		try {
			conn.close();
			stat.close();
		} catch (SQLException e) {
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
	
	public String getDB() {
		return db;
	}

	public void setDB(String db) {
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
		return this.db;
	}
}