/**
 * 
 */
/**
 * @author Sibo Wang
 *
 */

package conn;

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
import inputoutput.Table_Info;

public class DBConnector extends Connector {

	private static final Logger log = Logger.getLogger(DBConnector.class.getName());

	private String db_system_name;// db system name e.g., mysq/oracle etc.
	private String user_name;// db conn user name;
	private String password; // db conn password;
	private String conn_ip;// for database
	private int port;// db connection port
	Connection conn = null;
	private Table_Info tb_info;
	private Statement stat;	
	private long curr_offset=0;
	public DBConnector(){
		this.tb_info = new Table_Info();
	}
	
	public DBConnector(String db_system_name, String conn_ip, int port,
			String connectPath, String filename, String user_name, String password){
		this.db_system_name = db_system_name;
		this.conn_ip = conn_ip;
		this.port = port;
		this.connectPath = connectPath;
		this.filename = filename;
		this.user_name =user_name;
		this.password = password;
		this.tb_info = new Table_Info();
	}


	@Override
	public void initConnector() throws IOException {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:" + db_system_name + "://" + conn_ip + ":" + port + connectPath,
					user_name, password);

			List<Attribute> attrs = this.getAttributes();
			this.tb_info.setTable_attributes(attrs);

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
		String sql = "select * from "+filename+ " LIMIT "+ curr_offset+","+num;
		//String sql = "select * from "+filename+ " LIMIT "+ num;

		System.out.println(sql);
		ResultSet rs = stat.executeQuery(sql);
		boolean new_row = false;
		while(rs.next()){
			new_row = true;
			Record rec = new Record();
			for(int i=0; i<this.tb_info.getTable_attributes().size(); i++){
				String v1 = rs.getObject(i+1).toString();				
				//System.out.println(v1);
				rec.getTuples().add(v1);
			}
			rec_list.add(rec);
			System.out.println(rec);
		}
		curr_offset+=rec_list.size();
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
		DatabaseMetaData metadata = conn.getMetaData();
		ResultSet resultSet = metadata.getColumns(null, null, filename, null);
		Vector<Attribute> attrs  = new Vector<Attribute>();
		while (resultSet.next()) {
			String name = resultSet.getString("COLUMN_NAME");
			String type = resultSet.getString("TYPE_NAME");
			int size = resultSet.getInt("COLUMN_SIZE");
			Attribute attr = new Attribute(name, type, size);
			attrs.addElement(attr);
		}
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
		return this.filename;
	}
	
	public void setFilename(String filename){
		this.filename = filename;
	}
	
	public String getDb_system_name() {
		return db_system_name;
	}

	public void setDb_system_name(String db_system_name) {
		this.db_system_name = db_system_name;
	}

	public String getUser_name() {
		return user_name;
	}

	public void setUser_name(String user_name) {
		this.user_name = user_name;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getConn_ip() {
		return conn_ip;
	}

	public void setConn_ip(String conn_ip) {
		this.conn_ip = conn_ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public Table_Info getTb_info() {
		return tb_info;
	}

	public void setTb_info(Table_Info tb_info) {
		this.tb_info = tb_info;
	}

	public Statement getStat() {
		return stat;
	}

	public void setStat(Statement stat) {
		this.stat = stat;
	}
}
