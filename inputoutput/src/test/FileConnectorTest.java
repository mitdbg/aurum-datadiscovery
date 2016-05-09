package test;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.junit.Test;

import conn.FileConnector;
import inputoutput.Attribute;
import inputoutput.Record;

public class FileConnectorTest {

	@Test
	public void testGetAttributes() throws IOException {
		FileConnector fc = new FileConnector(".\\src\\test\\", "nell.simple.csv", " \t");
		Vector<Record> rec_list = new Vector<Record>();
		List<Attribute> attributes_of_table = fc.getAttributes();

		for(int i=0; i<attributes_of_table.size(); i++){
			System.out.println(attributes_of_table.get(i));
		}
		
		while (fc.readRows(100, rec_list)) {
			/*for (int j = 0; j < rec_list.size(); j++) {
				for (int i = 0; i < attributes_of_table.size(); i++) {
					System.out.println(attributes_of_table.get(i).getColumn_name() + ":" + rec_list.get(j).getTuples().get(i));
				}
			}*/
			rec_list = new Vector<Record>();
		}

		// FileConnector fc = new FileConnector();
		//System.out.println(fc);
	}
}
