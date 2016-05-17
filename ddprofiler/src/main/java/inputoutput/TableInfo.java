/**
 * @author Sibo Wang
 * @author ra-mit (edits)
 *
 */
package inputoutput;

import java.util.List;


public class TableInfo {
	
	private List<Attribute> tableAttributes;
	
	public TableInfo(){
		
	}
	public List<Attribute> getTableAttributes() {
		return tableAttributes;
	}
	public void setTableAttributes(List<Attribute> tableAttributes) {
		this.tableAttributes = tableAttributes;
	}
}
