package test;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;

import inputoutput.ColumnReader;
import inputoutput.Record;

public class ColumnReaderTest {

	@Test
	public void test() {
		
		//testing FileConnector
		ColumnReader cr = new ColumnReader();
		cr.initConfig(".\\src\\main\\java\\config\\fileconnector.config");
		//cr.initConfig(".\\src\\config\\dbconnector.config");
		cr.initConnector(cr.getConfig());	
		
		/*List<Record> recs = cr.readRecords(5);
		for(int i=0; i<recs.size(); i++){
			System.out.println(recs.get(i));
		}*/
		
		Iterator<Record> iter = cr.iterator();
		int num_rows=0;
		while(iter.hasNext()){
			System.out.println(iter.next());
			num_rows++;
		}
		
		assertEquals(num_rows, 46);
		
		
		//testing DBConnector 
		//this requires configuration of database, so it might go wrong
		//and uncomment it when setup is correct
		
		/*ColumnReader cr_db = new ColumnReader();
		cr_db.initConfig(".\\config\\dbconnector.config");
		cr_db.initConnector(cr.getConfig());
		List<Record> recs = cr.readRecords(5);
		for(int i=0; i<recs.size(); i++){
			System.out.println(recs.get(i));
		}*/
	}
}
