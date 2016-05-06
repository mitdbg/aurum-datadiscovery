package test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.junit.Test;

import conn.DBConnector;
import inputoutput.Attribute;
import inputoutput.Record;

public class DBConnectorTest {

	@Test
	public void test() {
		DBConnector conn = new DBConnector();
		conn.setDb_system_name("mysql");
		conn.setConn_ip("localhost");
		conn.setPort(3306);
		conn.setUser_name("root");
		conn.setPassword("Qatar");
		conn.setConnectPath("/test");
		conn.setFilename("nellsimple");
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