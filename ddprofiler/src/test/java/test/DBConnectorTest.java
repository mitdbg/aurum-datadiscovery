package test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.junit.Test;

import inputoutput.Attribute;
import inputoutput.Record;
import inputoutput.conn.DBConnector;
import inputoutput.conn.DBType;

public class DBConnectorTest {

	@Test
	public void test() {
		DBConnector conn = new DBConnector();
		
		conn.setDB(DBType.MYSQL);
		conn.setConnIP("localhost");
		conn.setPort("3306");
		conn.setUsername("root");
		conn.setPassword("Qatar");
		conn.__setConnectPath("/test");
		conn.setFilename("deathcause");
		try {
			conn.initConnector();
			List<Attribute> attr_list = conn.getAttributes();
			for (int i = 0; i < attr_list.size(); i++) {
				System.out.println(attr_list.get(i));
			}
			List<Record> rec_list = new Vector<Record>();
			while (conn.readRows(10, rec_list)) {
				int counter = 0;

				for (int i = 0; i < rec_list.size(); i++) 
					counter++;
					
				System.out.println(counter);
				rec_list = new Vector<Record>();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
