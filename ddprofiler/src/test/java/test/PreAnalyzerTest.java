package test;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import inputoutput.Attribute;
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
	}

}
