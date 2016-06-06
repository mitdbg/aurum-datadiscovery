/**
 * @author Sibo Wang
 * @author ra-mit (edits)
 *
 */

package inputoutput.conn;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import au.com.bytecode.opencsv.CSVReader;

import inputoutput.Attribute;
import inputoutput.Record;
import inputoutput.TableInfo;

public class FileConnector extends Connector {
	
	private CSVReader fileReader;
	private long lineCounter = 0;
	private TableInfo tableInfo;

	private char lineSplitter;
	private Vector<Record> records;
	
	public FileConnector() {
		this.lineSplitter = ',';
	}	
	
	public FileConnector(String connectPath, String filename, String spliter) throws IOException {
		this.connectPath = connectPath;
		this.sourceName = filename;
		
		/*
		 * FIXME: OpenCSV only support single spliter.  So we only use the first
		 * spliter in the splitter string, which may contain more than one char.
		 */
			 
		this.lineSplitter = spliter.charAt(0);
		
		this.tableInfo = new TableInfo();
		initConnector();
		List<Attribute> attrs = this.getAttributes();
		tableInfo.setTableAttributes(attrs);
	}
	
	@Override
	public String getSourceName() {
		return this.sourceName;
	}
	
	@Override
	void initConnector() throws FileNotFoundException {
		fileReader = new CSVReader(new FileReader(connectPath+sourceName), this.lineSplitter);
		//fileReader = new BufferedReader(new FileReader(connectPath+sourceName));
	}
	
	void destroyConnector(){
		try {
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	public List<Attribute> getAttributes() throws IOException {
		
		//assume that the first row is the attributes;
		
		if(lineCounter!=0){
			//wrong usage of the getAttributes function
			return tableInfo.getTableAttributes();
		}
		String[] attributes = fileReader.readNext();
		lineCounter++;
		//List<String> attr_name_list =  csv_spliter(attributes);
		
		Vector<Attribute> attr_list = new Vector<Attribute>();
		for(int i=0; i< attributes.length; i++){
			Attribute attr = new Attribute(attributes[i]);
			attr_list.addElement(attr);
		}
		return attr_list;
	}
	
	
	public Vector<Record> getRecords(int num){
		return records;
	}
	
	@Override
	public boolean readRows(int num, List<Record> rec_list) throws IOException {
		boolean read_lines = false;
		String[] res = null;
		for(int i=0; i<num && (res = fileReader.readNext()) != null; i++) {
			lineCounter++;
			read_lines = true;
			Record rec = new Record();
			rec.setTuples(res);
			rec_list.add(rec);
		}
		return read_lines;
	}

	int debug_cnt=0;


	
	/*
	 * obsoleted function, buggy when handling cvs files like below
	 * A,B,"C DDD "" EEE, F
	 * it should be splited into (|A|, |B|, |"C DDD "" EEE|, F). 
	 * however, current function will return  (|A|, |B|, |"C DDD "" EEE, F|) since it escape comma inside quotes.
	 */
	@Deprecated
	public List<String> csv_spliter(String attributes){
		Vector<String> results = new Vector<String>();
		int start_pos = 0;
		boolean inside_qutation=false;
		for(int i=0; i<attributes.length(); i++){
			
			if(lineSplitter == attributes.charAt(i) && inside_qutation == false){
				String store_str = attributes.substring(start_pos, i);
				results.addElement(store_str.trim());				
				start_pos = i+1;
			}
			
			if(attributes.charAt(i) == '\"'){
				inside_qutation = !inside_qutation;
			}			
		}
		String last_str = attributes.substring(start_pos, attributes.length());
		results.addElement(last_str.trim());
		return results;
	}
	
	
}
