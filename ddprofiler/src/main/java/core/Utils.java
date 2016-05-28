package core;

public class Utils {

	public static int computeAttrId(String sourceName, String columnName) {
		String t = sourceName.concat(columnName);
		return t.hashCode();
	}
	
}
