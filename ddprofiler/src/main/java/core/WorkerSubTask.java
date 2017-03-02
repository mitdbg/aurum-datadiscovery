package core;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import inputoutput.Attribute;
import inputoutput.Tracker;
import inputoutput.conn.DBConnector;
import inputoutput.conn.DBType;
import inputoutput.conn.Connector;
import inputoutput.conn.FileConnector;
import preanalysis.Values;

public class WorkerSubTask {

  private final int taskId;
  private final Tracker tracker;
  private final Map<Attribute, Values> values;

  private final String dbName;
  private final String path;
  private final String sourceName;


  public WorkerSubTask(int id, Tracker tracker, Map<Attribute, Values> values,
                       String dbName, String path, String sourceName) {
    this.taskId = id;
    this.tracker = tracker;
    this.values = values;
    this.dbName = dbName;
    this.path = path;
    this.sourceName = sourceName;
  }

  public int getTaskId() { return taskId; }

  public Tracker getTracker() { return this.tracker; }

  public Map<Attribute, Values> getValues() { return this.values; }

  public String getDBName() { return this.dbName; }

  public String getPath() { return this.path; }

  public String getSourceName() { return this.sourceName; }

}
