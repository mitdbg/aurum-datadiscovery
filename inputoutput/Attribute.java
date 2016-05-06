package inputoutput;

public class Attribute {
	private String column_name;
	private String column_type;
	private int column_size;

	public Attribute(String column_name){
		this.column_name = column_name;
		this.column_type = "UNKNOWN";
		this.column_size = -1;
	}
	
	public Attribute(String column_name, String column_type, int column_size){
		this.column_name = column_name;
		this.column_type = column_type;
		this.column_size = column_size;
	}
	
	public String getColumn_name() {
		return column_name;
	}
	public void setColumn_name(String column_name) {
		this.column_name = column_name;
	}
	public String getColumn_type() {
		return column_type;
	}
	public void setColumn_type(String column_type) {
		this.column_type = column_type;
	}
	public int getColumn_size() {
		return column_size;
	}
	public void setColumn_size(int column_size) {
		this.column_size = column_size;
	}
	
	public String toString(){
		return "column name: " + this.column_name + " column type: " + this.column_type 
				+ " column size: "+ this.column_size;
	}
}
