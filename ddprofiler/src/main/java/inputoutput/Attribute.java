/**
 * @author Sibo Wang
 * @author ra-mit (edits)
 */
package inputoutput;

import analysis.Analysis;
import analysis.AnalyzerFactory;
import analysis.modules.EntityAnalyzer;

public class Attribute {

  public enum AttributeType {
    INT,
    FLOAT,
    LONG,
    STRING,
    UNKNOWN;
  }

  private static final int pseudoRandomSeed = 1;

  private String columnName;
  private AttributeType columnType;
  private int columnSize;
  private Analysis analyzer;
  private final Tracker tracker;

  public Attribute(String column_name) {
    this.columnName = column_name;
    this.columnType = AttributeType.UNKNOWN;
    this.columnSize = -1;
    this.analyzer = null;
    this.tracker = new Tracker();
  }

  public Attribute(String column_name, String column_type, int column_size) {
    this.columnName = column_name;
    // TODO: swith(column_type) and transform string into enum
    this.columnType = AttributeType.UNKNOWN;
    this.columnSize = column_size;
    this.analyzer = null;
    this.tracker = new Tracker();
  }

  public String getColumnName() { return columnName; }

  public void setColumnName(String column_name) {
    this.columnName = column_name;
  }

  public AttributeType getColumnType() { return columnType; }

  public Analysis getAnalyzer() { return analyzer; }

  public Tracker getTracker() { return tracker; }

  public void setColumnType(AttributeType columnType) {
    this.columnType = columnType;
    if(columnType.equals(AttributeType.FLOAT)) {
      analyzer = AnalyzerFactory.makeNumericalAnalyzer();
    }
    else if(columnType.equals(AttributeType.INT)) {
      analyzer = AnalyzerFactory.makeNumericalAnalyzer();
    }
    else if(columnType.equals(AttributeType.STRING)) {
      analyzer = AnalyzerFactory.makeTextualAnalyzer(pseudoRandomSeed);
    }
  }

  public int getColumnSize() { return columnSize; }

  public void setColumnSize(int column_size) { this.columnSize = column_size; }

  public String toString() {
    return "column name: " + this.columnName + " column type: " +
        this.columnType + " column size: " + this.columnSize;
  }
}
