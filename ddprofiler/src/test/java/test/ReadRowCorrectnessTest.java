package test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.junit.Test;

import inputoutput.Attribute;
import inputoutput.conn.FileConnector;

public class ReadRowCorrectnessTest {
	private String path1 = "C:\\csv\\";
	private String filename1="dataset1.csv";

	private String path2 = "C:\\csv2\\";
	private String filename2="dataset1.csv";

	//private String path = "/Users/ra-mit/Desktop/mitdwh_test/";
	//private String filename = "short_cis_course_catalog.csv";
	private String separator = ",";
	int numRecords = 100;

	@Test
	public void readRowCorrectnesstest() throws IOException, SQLException {
		FileConnector fc1 = new FileConnector(path1, filename1, separator);
		FileConnector fc2 = new FileConnector(path2, filename2, separator);

		Map<Attribute, List<String>> data1  = fc1.readRows(numRecords);
		Map<Attribute, List<String>> data2  = fc2.readRowsBaseline(numRecords);
		
		Vector<List<String>> compareList1 = new Vector<List<String>>();
		for(List<String> val : data1.values())
			compareList1.add(val);
		Vector<List<String>> compareList2 = new Vector<List<String>>();
		for(List<String> val: data2.values())
			compareList2.add(val);
		assertEquals(compareList1.size(), compareList2.size());
		for(int i=0; i < compareList1.size(); i++)
			for(int j=0; j<compareList1.get(i).size(); j++){
				String val1 = compareList1.get(i).get(j);
				String val2 = compareList2.get(i).get(j);
				assertEquals(val1, val2);
				System.out.println(val1+" vs "+ val2);
			}
		
		
	}

}
