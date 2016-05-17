package test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import inputoutput.conn.FileConnector;
import preanalysis.PreAnalyzer;

public class PreAnalyzerTest {

	private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
	private String filename = "short_cis_course_catalog.csv";
	private String separator = ",";
	private int numRecords = 100;
	
	@Test
	public void testPreAnalyzerForTypes() throws IOException {
				
		FileConnector fc = new FileConnector(path, filename, separator);
		
		PreAnalyzer pa = new PreAnalyzer();
		pa.composeConnector(fc);
		
		pa.readRows(numRecords);
		
		List<Attribute> attrs = pa.getEstimatedDataTypes();
		for(Attribute a : attrs) {
			System.out.println(a);
		}
		
		Map<Attribute, List<Object>> data = pa.readRows(numRecords);
		for(Entry<Attribute, List<Object>> a : data.entrySet()) {
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println("NAME: " + a.getKey().getColumnName());
			System.out.println("TYPE: " + a.getKey().getColumnType());
			if(a.getKey().getColumnType().equals(AttributeType.FLOAT)) {
				List<Object> objs = a.getValue();
				for(Object f : objs) {
					System.out.println((float)f);
				}
			}
			if(a.getKey().getColumnType().equals(AttributeType.STRING)) {
				List<Object> objs = a.getValue();
				for(Object f : objs) {
					System.out.println(f);
				}
			}
		}
	}

}
