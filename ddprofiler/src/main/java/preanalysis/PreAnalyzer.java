/**
 * @author ra-mit
 */
package preanalysis;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import inputoutput.conn.Connector;

public class PreAnalyzer implements PreAnalysis, IO {

	private Connector c;
	private List<Attribute> attributes;
	private boolean knownDataTypes = false;
	
	/**
	 * Implementation of IO interface
	 */
	
	@Override
	public Map<Attribute, List<String>> readRows(int num) {
		Map<Attribute, List<String>> data = null;
		try {
			data = c.readRows(num);
		}
		catch (IOException | SQLException e) {
			e.printStackTrace();
		}
		
		if(! knownDataTypes) {
			calculateDataTypes(data);
		}
		
		// TODO: update quality report
		
		return data;
	}
	
	private void calculateDataTypes(Map<Attribute, List<String>> data) {
		// estimate data type for each attribute
		for(Entry<Attribute, List<String>> e : data.entrySet()) {
			Attribute a = e.getKey();
			// Only if the type is not already known
			if(! a.getColumnType().equals(AttributeType.UNKNOWN)) continue;
			AttributeType aType = typeOfValue(e.getValue());
			a.setColumnType(aType);
		}
	}
	
	/**
	 * FIXME: Will always choose FLOAT or STRING. fix it to choose INT when appropriate 
	 * @param values
	 * @return
	 */
	private AttributeType typeOfValue(List<String> values) {
		boolean isFloat = false;
		boolean isInt = false;
		int floatMatches = 0;
		int intMatches = 0;
		int strMatches = 0;
		for(String s : values) {
			try {
		      Float.valueOf(s.trim()).floatValue();
		      floatMatches++;
		    }
		    catch (NumberFormatException nfe) {
		    	try {
		    		Integer.valueOf(s.trim()).intValue();
		    		intMatches++;
				}
				catch (NumberFormatException nfe2) {
					strMatches++;
				}
		    }
		}
		if(floatMatches > 0 && intMatches == 0) isFloat = true;
		if(intMatches > 0 && strMatches == 0) isInt = true;
		if(isFloat) return AttributeType.FLOAT;
		if(isInt) return AttributeType.INT;
		return AttributeType.STRING;
	}
	
	/**
	 * Implementation of PreAnalysis interface
	 */

	@Override
	public void composeConnector(Connector c) {
		this.c = c;
		try {
			this.attributes = c.getAttributes();
		} 
		catch (IOException | SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public DataQualityReport getQualityReport() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Attribute> getEstimatedDataTypes() {
		return attributes;
	}
	
}
