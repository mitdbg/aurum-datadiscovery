/**
 * @author Sibo Wang
 * @author ra-mit (edits)
 *
 */

package inputoutput.conn;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import inputoutput.Attribute;
import inputoutput.Record;

public abstract class Connector {

	protected String connectPath;//the path directory in the database or file system
	protected String filename;// the table in the database or the cvs file name

	abstract void initConnector() throws IOException, ClassNotFoundException, SQLException;
	abstract void destroyConnector();
	public abstract List<Attribute> getAttributes() throws IOException, SQLException;
	public abstract boolean readRows(int num, List<Record> rec_list) throws IOException, SQLException;
	public abstract Map<Attribute, List<String>> readRows(int num) throws IOException, SQLException;
	
}