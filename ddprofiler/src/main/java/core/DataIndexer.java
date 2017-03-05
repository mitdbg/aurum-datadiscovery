package core;

import java.util.Map;

import inputoutput.Attribute;
import preanalysis.Values;

public interface DataIndexer {

  public boolean indexData(String dbName, String path, Attribute a, Values values);
  public boolean flushAndClose();
}
