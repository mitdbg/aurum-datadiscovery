package store;

import java.util.List;

import core.WorkerTaskResult;

public interface Store {

  public void initStore();
  public boolean indexData(int id, String dbName, String sourceName, String columnName,
                           List<String> values);
  public boolean storeDocument(WorkerTaskResult wtr);
  public void tearDownStore();
}
