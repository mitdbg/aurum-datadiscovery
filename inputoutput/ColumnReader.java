/**
 * 
 */
/**
 * @author Sibo Wang
 *
 */
package inputoutput;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import conn.Connector;
import conn.FileConnector;

public class ColumnReader {

	/**
	 * @param args
	 */
	private static final Logger log= Logger.getLogger( ColumnReader.class.getName() );

	private Connector conn;
	private Config config;
	
	public ColumnReader(){
		config = new Config();
	}
	
	public void initConnector(Config config){
		if(config.getConn_type().equals("FILE")){
			log.info("The connector is FileConnector");
			try {
				conn = new FileConnector(config.getConn_path(), config.getConn_filename(), ',');
			} catch (IOException e) {
				log.log(Level.SEVERE, "Cannot initlaize the file connector");
				e.printStackTrace();
			}
		}
	}
	
	public void initConfig(String filename){
		try {
			config.load_config_file(filename);
		} catch (IOException e) {
			log.log(Level.SEVERE, "Configure file not found  ", filename);
			e.printStackTrace();
		}
	}
	
	public Config getConfig(){
		return this.config;
	}
	
	public List<Record> readRecords(int num_records){
		try {
			Vector<Record> rec_list = new Vector<Record>();
			boolean read_success = conn.readRows(num_records, rec_list);
			if(read_success)
				return rec_list;
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error occured when reading csv records", e);
		} catch (SQLException e) {
			log.log(Level.SEVERE, "Error occured when reading database records", e);
			e.printStackTrace();
		}
		return null;
	}
	
	public static void main(String[] args) {

	}

}
