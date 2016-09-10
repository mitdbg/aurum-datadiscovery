package core;

import java.util.Map;

import inputoutput.Attribute;
import preanalysis.Values;

public interface DataIndexer {

  public boolean indexData(String dbName, Map<Attribute, Values> data);
  public boolean flushAndClose();
}
