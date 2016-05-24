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
import preanalysis.Values;

public class PreAnalyzerTest {

	private String path = "C:/";
	private String filename = "Leading_Causes_of_Death__1990-2010.csv";
	//private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
	//private String filename = "short_cis_course_catalog.csv";
	private String separator = ",";
	private int numRecords = 10;
	
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
		
		Map<Attribute, Values> data = pa.readRows(numRecords);
		for(Entry<Attribute, Values> a : data.entrySet()) {
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println("NAME: " + a.getKey().getColumnName());
			System.out.println("TYPE: " + a.getKey().getColumnType());
			if(a.getKey().getColumnType().equals(AttributeType.FLOAT)) {
				List<Float> objs = a.getValue().getFloats();
				for(float f : objs) {
					System.out.println(f);
				}
			}
			if(a.getKey().getColumnType().equals(AttributeType.STRING)) {
				List<String> objs = a.getValue().getStrings();
				for(String f : objs) {
					System.out.println(f);
				}
			}
		}
	}

}
