/**
 * @author ra-mit
 */
package preanalysis;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import inputoutput.Attribute;
import inputoutput.conn.Connector;

public class PreAnalyzer implements PreAnalysis, IO {

	private Connector c;
	private List<Attribute> attributes;
	
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// TODO: do preanalysis and update internal structures
		
		return data;
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
	public List<Attribute> estimateDataType() {
		return attributes;
	}
	
}
