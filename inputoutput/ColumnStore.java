/**
 * 
 */
/**
 * @author Sibo Wang
 *
 */
package inputoutput;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class ColumnStore {
	public ConcurrentHashMap<String, Vector<String>> getColumn_vectors() {
		return column_vectors;
	}
	public void setColumn_vectors(ConcurrentHashMap<String, Vector<String>> column_vectors) {
		this.column_vectors = column_vectors;
	}
	public Vector<String> getColumn_index() {
		return column_index;
	}
	public void setColumn_index(Vector<String> column_index) {
		this.column_index = column_index;
	}
	protected ConcurrentHashMap<String, Vector<String>> column_vectors;
	protected Vector<String> column_index;
	public ColumnStore(List<String> tb_attr){
		column_vectors = new ConcurrentHashMap<String, Vector<String>>();
		column_index = new Vector<String>();
		for(int i=0; i<tb_attr.size(); i++){
			Vector<String> col_vec = new Vector<String>();
			String attr_name = tb_attr.get(i);
			column_vectors.put(attr_name, col_vec);
			column_index.addElement(attr_name);
		}
	}
}
