package test;

import static org.junit.Assert.*;

import org.junit.Test;

import inputoutput.ColumnReader;

public class ColumnReaderTest {

	@Test
	public void test() {
		ColumnReader cr = new ColumnReader();
		cr.initConfig(".\\config\\fileconnector.config");
		cr.initConnector(cr.getConfig());	
	}
}
