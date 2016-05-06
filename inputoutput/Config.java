/**
 * 
 */
/**
 * @author Sibo Wang
 *
 */

package inputoutput;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Config {
	private static final Logger log= Logger.getLogger( Config.class.getName() );
	public String getConn_type() {
		return conn_type;
	}
	public void setConn_type(String conn_type) {
		this.conn_type = conn_type;
	}
	public String getConn_path() {
		return conn_path;
	}
	public void setConn_path(String conn_path) {
		this.conn_path = conn_path;
	}
	public String getConn_filename() {
		return conn_filename;
	}
	public void setConn_filename(String conn_filename) {
		this.conn_filename = conn_filename;
	}
	public String getConn_ip() {
		return conn_ip;
	}
	public void setConn_ip(String conn_ip) {
		this.conn_ip = conn_ip;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
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
	public String getDb_system_name() {
		return db_system_name;
	}
	public void setDb_system_name(String db_system_name) {
		this.db_system_name = db_system_name;
	}
	
	private String db_system_name;// db system name e.g., mysq/oracle etc.
	private String user_name;//db conn user name;
	private String password; //db conn password;
	private String conn_type;//whether it is file connector or db connector
	private String conn_path;//for file dir (csv) or database dir (in database)
	private String conn_filename;//for file name (csv) or table (in database)
	private String conn_ip;//for database
	private String port;//db connection port
	
	public void load_config_file(String config_file) throws IOException{
		BufferedReader bf_reader = new BufferedReader(new FileReader(config_file));
		log.info("loading the configure file");
		conn_type = bf_reader.readLine().trim();
		if(conn_type.equals("FILE")){
			conn_path = bf_reader.readLine();
			conn_filename = bf_reader.readLine();
		}else if(conn_type.equals("DB")){
			conn_ip = bf_reader.readLine();
			conn_path = bf_reader.readLine();
			conn_filename = bf_reader.readLine();
			db_system_name = bf_reader.readLine();
			port = bf_reader.readLine();
			user_name = bf_reader.readLine();
			password = bf_reader.readLine();
		}else{
			log.log(Level.SEVERE, "Incorrect configuration file with configure type",  conn_type);
		}
	}

}
