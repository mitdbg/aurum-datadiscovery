/**
 * @author Sibo Wang
 * @author ra-mit (edits)
 *
 */

package inputoutput.conn;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import inputoutput.Attribute;
import inputoutput.Record;
import inputoutput.TableInfo;

public class FileConnector extends Connector {
	
	private BufferedReader fileReader;
	private long lineCounter = 0;
	private TableInfo tableInfo;

	private String lineSplitter;
	private Vector<Record> records;
	
	public FileConnector() {
		this.lineSplitter = ",";
	}	
	
	public FileConnector(String connectPath, String filename, String spliter) throws IOException {
		this.connectPath = connectPath;
		this.filename = filename;
		this.lineSplitter = spliter;
		this.tableInfo = new TableInfo();
		initConnector();
		List<Attribute> attrs = this.getAttributes();
		tableInfo.setTableAttributes(attrs);
	}
	
	@Override
	void initConnector() throws FileNotFoundException {
		fileReader = new BufferedReader(new FileReader(connectPath+filename));
	}
	
	void destroyConnector(){
		try {
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public List<String> csv_spliter(String attributes){
		Vector<String> results = new Vector<String>();
		int start_pos = 0;
		boolean inside_qutation=false;
		for(int i=0; i<attributes.length(); i++){
			
			if(lineSplitter.indexOf(attributes.charAt(i))>=0 && inside_qutation == false){
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
	
	public List<Attribute> getAttributes() throws IOException {
		
		//assume that the first row is the attributes;
		
		if(lineCounter!=0){
			//wrong usage of the getAttributes function
			return tableInfo.getTableAttributes();
		}
		String attributes = fileReader.readLine();
		lineCounter++;
		List<String> attr_name_list =  csv_spliter(attributes);
		Vector<Attribute> attr_list = new Vector<Attribute>();
		for(int i=0; i< attr_name_list.size(); i++){
			Attribute attr = new Attribute(attr_name_list.get(i));
			attr_list.addElement(attr);
		}
		return attr_list;
	}
	
	
	public Vector<Record> getRecords(int num){
		return records;
	}
	
	@Override
	public boolean readRows(int num, List<Record> rec_list) throws IOException {
		String line;
		boolean read_lines = false;

		for(int i=0; i<num && (line = fileReader.readLine())!=null; i++ ){
			lineCounter++;
			read_lines = true;
			List<String> res = csv_spliter(line); 
			//System.out.println(res.size());
			Record rec = new Record();
			rec.setTuples(res);
			rec_list.add(rec);
			/*for(int j=0; j<res.size(); j++){
				String attr = col_store.getColumn_index().get(j);
				col_store.getColumn_vectors().get(attr).add(res.get(j));
			}*/
		}
		return read_lines;
	}


	/**
	 * Returns a map with Attribute of table as key and a list of num values as value.
	 * Map is created internally as it must preserve insertion order
	 * @param num
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	@Override
	public Map<Attribute, List<String>> readRows(int num) throws IOException, SQLException {
		
		Map<Attribute, List<String>> data = new LinkedHashMap<>();
		// Make sure attrs is populated, if not, populate it here
		if(data.isEmpty()) {
			List<Attribute> attrs = this.tableInfo.getTableAttributes();
			attrs.forEach(a -> data.put(a, new ArrayList<>()));
		}
		
		// Read data and insert in order
		List<Record> recs = new ArrayList<>();
		boolean readData = this.readRows(num, recs);
		if(!readData) {
			return null;
		}
		for(Record r : recs) {
			List<String> values = r.getTuples();
			int currentIdx = 0;
			for(List<String> vals : data.values()) { // ordered iteration
				vals.add(values.get(currentIdx));
				currentIdx++;
			}
		}
		
		return data;
	}

}
