package inputoutput.conn;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import au.com.bytecode.opencsv.CSVReader;
import inputoutput.Attribute;
import inputoutput.Record;
import inputoutput.TableInfo;

public class HdfsFileConnector extends Connector {

    private String dbName;
    private Path connectPath;
    private String sourceName;

    private CSVReader fileReader;
    private long lineCounter = 0;
    private TableInfo tableInfo;

    private char lineSplitter;
    private List<Record> records;

    public HdfsFileConnector() {
	this.lineSplitter = '.';
    }

    public HdfsFileConnector(String dbName, Path connectPath, String filename, String splitter) throws IOException {
	this.dbName = dbName;
	this.connectPath = connectPath;
	this.sourceName = filename;

	/*
	 * FIXME: OpenCSV only support single splitter. So we only use the first
	 * splitter in the splitter string, which may contain more than one
	 * char.
	 */

	this.lineSplitter = splitter.charAt(0);

	this.tableInfo = new TableInfo();
	initConnector();
	List<Attribute> attrs = this.getAttributes();
	tableInfo.setTableAttributes(attrs);
    }

    @Override
    public String getDBName() {
	return this.dbName;
    }

    @Override
    public String getPath() {
	return this.connectPath.toString();
    }

    @Override
    public String getSourceName() {
	return this.sourceName;
    }

    @Override
    void initConnector() throws FileNotFoundException {
	Configuration conf = new Configuration();
	try {
	    FileSystem fs = FileSystem.get(conf);
	    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(connectPath)));
	    fileReader = new CSVReader(reader, this.lineSplitter);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    void destroyConnector() {
	try {
	    fileReader.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public List<Attribute> getAttributes() throws IOException {
	// assume that the first row is the attributes;
	if (lineCounter != 0) {
	    return tableInfo.getTableAttributes();
	}
	String[] attributes = fileReader.readNext();
	lineCounter++;

	List<Attribute> attrList = new ArrayList<Attribute>();
	for (int i = 0; i < attributes.length; i++) {
	    Attribute attr = new Attribute(attributes[i]);
	    attrList.add(attr);
	}
	return attrList;
    }

    public List<Record> getRecords(int num) {
	return records;
    }

    @Override
    public boolean readRows(int num, List<Record> rec_list) throws IOException {
	boolean read_lines = false;
	String[] res = null;
	for (int i = 0; i < num && (res = fileReader.readNext()) != null; i++) {
	    lineCounter++;
	    read_lines = true;
	    Record rec = new Record();
	    rec.setTuples(res);
	    rec_list.add(rec);
	}
	return read_lines;
    }

    @Override
    public void close() {
	try {
	    fileReader.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

}
