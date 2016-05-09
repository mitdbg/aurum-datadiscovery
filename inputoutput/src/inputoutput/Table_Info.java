/**
 * 
 */
/**
 * @author Sibo Wang
 *
 */
package inputoutput;

import java.util.List;


public class Table_Info {
	private List<Attribute> table_attributes;
	public Table_Info(){
		//table_attributes = new Vector<Attribute>();
		//guess_table_attributes = new Vector<String>();
	}
	public List<Attribute> getTable_attributes() {
		return table_attributes;
	}
	public void setTable_attributes(List<Attribute> table_attributes) {
		this.table_attributes = table_attributes;
	}
}
