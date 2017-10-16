package core;

import inputoutput.Attribute;
import inputoutput.Tracker;
import preanalysis.Values;

public class WorkerSubTask {

  private final int taskId;
  private final Attribute attribute;
  private final Values values;

  private final String dbName;
  private final String path;
  private final String sourceName;


  public WorkerSubTask(int id, Attribute attribute, Values values,
                       String dbName, String path, String sourceName) {
    this.taskId = id;
    this.attribute = attribute;
    this.values = values;
    this.dbName = dbName;
    this.path = path;
    this.sourceName = sourceName;
  }

  public int getTaskId() { return taskId; }

  public Tracker getTracker() { return this.attribute.getTracker(); }

  public Attribute getAttribute() { return this.attribute; }

  public Values getValues() { return this.values; }

  public String getDBName() { return this.dbName; }

  public String getPath() { return this.path; }

  public String getSourceName() { return this.sourceName; }

}
