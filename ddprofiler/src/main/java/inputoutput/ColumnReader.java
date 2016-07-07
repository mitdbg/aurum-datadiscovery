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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import inputoutput.conn.DBConnector;
import inputoutput.conn.Connector;
import inputoutput.conn.FileConnector;

public class ColumnReader implements Iterable<Record> {

	/**
	 * @param args
	 */
	private static final Logger log = Logger.getLogger(ColumnReader.class.getName());

	private Connector conn;
	private Config config;

	public ColumnReader() {
		config = new Config();
	}

	public void initConnector(Config config) {
		System.out.println(config == null);
		if (config.getConn_type().equals("FILE")) {
			log.info("The connector is FileConnector");
			try {
				conn = new FileConnector(config.getConn_path(), config.getConn_filename(), config.getSpliter());
			} catch (IOException e) {
				log.log(Level.SEVERE, "Cannot initlaize the file connector");
				e.printStackTrace();
			}
		}else if (config.getConn_type().equals("DB")) {
			log.info("The connector is DBConnector");
			try {
				// FIXME: change null by the appropriate DBType enum object
				conn = new DBConnector(null, config.getConn_ip(), config.getPort(),
						config.getConn_path(), config.getConn_filename(), config.getUser_name(), config.getPassword());
			} catch (NumberFormatException | IOException e) {
				log.log(Level.SEVERE, "Cannot initlaize the DB connector");
				e.printStackTrace();
			}
		}else{
			log.log(Level.SEVERE, "Wrong connector type");
		}
	}

	public void initConfig(String filename) {
		try {
			config.load_config_file(filename);
		} catch (IOException e) {
			log.log(Level.SEVERE, "Configure file not found  ", filename);
			e.printStackTrace();
		}
	}

	public Config getConfig() {
		return this.config;
	}

	public List<Record> readRecords(int num_records) {
		try {
			Vector<Record> rec_list = new Vector<Record>();
			boolean read_success = conn.readRows(num_records, rec_list);
			if (read_success)
				return rec_list;
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error occured when reading csv records", e);
		} catch (SQLException e) {
			log.log(Level.SEVERE, "Error occured when reading database records", e);
			e.printStackTrace();
		}
		return null;
	}

	// define a constant to read row buffers, currently set to 1024
	final int ROW_BUF = 1024;

	@Override
	public Iterator<Record> iterator() {
		return new ColumnIterator();
	}

	private class ColumnIterator implements Iterator<Record> {
		private int cursor;
		private List<Record> record_buf;
		public ColumnIterator() {
			record_buf = readRecords(ROW_BUF);
			this.cursor = 0;
		}

		public boolean hasNext() {
			if(cursor < record_buf.size()){
				return true;
			}else{
				//record_buf has finished iteration
				record_buf = readRecords(ROW_BUF);
				if(record_buf == null) return false;
				this.cursor = 0;
			}
			return true;
		}

		public Record next() {
			if (this.hasNext()) {
				Record current = record_buf.get(cursor);
				cursor++;
				return current;
			}
			throw new NoSuchElementException();
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/*
	 * private Iterator<Record> curr_iterator; public Iterator<Record>
	 * columnIterator(){ if(curr_iterator == null){ List<Record> rec =
	 * readRecords(ROW_BUF); curr_iterator = rec.iterator(); return
	 * curr_iterator; }else{ //curr_iterator = curr_iterator.next(); return
	 * curr_iterator; } }
	 */

}
