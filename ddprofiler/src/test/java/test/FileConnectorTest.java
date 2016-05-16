package test;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.junit.Test;

import inputoutput.Attribute;
import inputoutput.Record;
import inputoutput.conn.FileConnector;

public class FileConnectorTest {

	private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
	private String filename = "short_cis_course_catalog.csv";
	private String separator = ",";
	private int numRecords = 100;
	
	@Test
	public void testGetAttributes() throws IOException {
		
		FileConnector fc = new FileConnector(path, filename, separator);
		Vector<Record> rec_list = new Vector<Record>();
		List<Attribute> attributes_of_table = fc.getAttributes();

		for(int i=0; i<attributes_of_table.size(); i++){
			System.out.println(attributes_of_table.get(i));
		}
		
		while (fc.readRows(numRecords, rec_list)) {
			for (int j = 0; j < rec_list.size(); j++) {
				for (int i = 0; i < attributes_of_table.size(); i++) {
					System.out.println(attributes_of_table.get(i).getColumn_name() + ":" + rec_list.get(j).getTuples().get(i));
				}
			}
			rec_list = new Vector<Record>();
		}
	}
}
