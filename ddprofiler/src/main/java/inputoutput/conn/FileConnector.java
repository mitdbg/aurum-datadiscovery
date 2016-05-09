/**
 * 
 */
/**
 * @author Sibo Wang
 *
 */

package inputoutput.conn;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import inputoutput.Attribute;
import inputoutput.Record;
import inputoutput.Table_Info;

public class FileConnector extends Connector{
	private BufferedReader fileReader;
	private long line_counter=0;
	private Table_Info tb_info;

	private String spliter;
	public FileConnector(){this.spliter = ",";}	
	private Vector<Record> records;
	
	public FileConnector(String connectPath, String filename, String spliter) throws IOException{
		this.connectPath = connectPath;
		this.filename = filename;
		this.spliter = spliter;
		this.tb_info = new Table_Info();
		initConnector();
		List<Attribute> attrs = this.getAttributes();
		tb_info.setTable_attributes(attrs);
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
			
			if(spliter.indexOf(attributes.charAt(i))>=0 && inside_qutation == false){
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
	
	public List<Attribute> getAttributes() throws IOException{
		
		//assume that the first row is the attributes;
		
		if(line_counter!=0){
			//wrong usage of the getAttributes function
			return tb_info.getTable_attributes();
		}
		String attributes = fileReader.readLine();
		line_counter++;
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
	public boolean readRows(int num, List<Record> rec_list) throws IOException{
		String line;
		boolean read_lines = false;
		/*for(String keys: col_store.getColumn_vectors().keySet()){
			col_store.getColumn_vectors().get(keys).clear();
		}*/
		 
		
		
		for(int i=0; i<num && (line = fileReader.readLine())!=null; i++ ){
			line_counter++;
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

}
