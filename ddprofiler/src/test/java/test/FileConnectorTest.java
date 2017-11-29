package test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.junit.Test;

import inputoutput.Attribute;
import inputoutput.Record;
import inputoutput.connectors.Old_FileConnector;

public class FileConnectorTest {

  private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
  private String filename = "short_cis_course_catalog.csv";
  private String separator = ",";
  private int numRecords = 100;

  @Test
  public void testGetAttributes() throws IOException {

    Old_FileConnector fc = new Old_FileConnector("", path, filename, separator);
    Vector<Record> rec_list = new Vector<Record>();
    List<Attribute> attributes_of_table = fc.getAttributes();

    for (int i = 0; i < attributes_of_table.size(); i++) {
      System.out.println(attributes_of_table.get(i));
    }

    while (fc.readRows(numRecords, rec_list)) {
      for (int j = 0; j < rec_list.size(); j++) {
        for (int i = 0; i < attributes_of_table.size(); i++) {
          System.out.println(attributes_of_table.get(i).getColumnName() + ":" +
                             rec_list.get(j).getTuples().get(i));
        }
      }
      rec_list = new Vector<Record>();
    }
  }

  @Test
  public void testGetAttrValueMap() throws IOException {

    Old_FileConnector fc = new Old_FileConnector("", path, filename, separator);

    int numRecords = 9;
    Map<Attribute, List<String>> data = null;
    try {
      data = fc.readRows(numRecords);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    for (Entry<Attribute, List<String>> e : data.entrySet()) {
      System.out.println(e.getKey());
      e.getValue().forEach(string -> System.out.print(string + ", "));
      System.out.println();
    }
    for (Entry<Attribute, List<String>> e : data.entrySet()) {
      System.out.println(e.getKey());
      System.out.println("numValues: " + e.getValue().size());
    }

    try {
      data = fc.readRows(numRecords);
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

    for (Entry<Attribute, List<String>> e : data.entrySet()) {
      System.out.println(e.getKey());
      e.getValue().forEach(string -> System.out.print(string + ", "));
      System.out.println();
    }
  }
}
