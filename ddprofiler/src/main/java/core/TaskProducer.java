package core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import inputoutput.Tracker;
import inputoutput.conn.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analysis.Analysis;
import analysis.AnalyzerFactory;
import analysis.NumericalAnalysis;
import analysis.TextualAnalysis;
import analysis.modules.EntityAnalyzer;
import core.config.ProfilerConfig;
import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import preanalysis.PreAnalyzer;
import preanalysis.Values;
import store.Store;

public class TaskProducer implements Worker {

    private final Logger LOG = LoggerFactory.getLogger(TaskProducer.class.getName());

    // Settings
    private final Conductor conductor;
    private final String workerName;
    private final int numRecordChunk;
    private boolean doWork = true;

    // The current task
    private WorkerTask task;

    public TaskProducer(Conductor conductor, ProfilerConfig pc, String workerName) {
        this.conductor = conductor;
        this.numRecordChunk = pc.getInt(ProfilerConfig.NUM_RECORD_READ);
        this.workerName = workerName;
    }

    public void stop() {
        this.doWork = false;
    }

    private void submitSubTasks(Map<Attribute, Values> data, String dbName, String path, String sourceName) {
        int seed = 0; // To get a unique task ID
        for (Entry<Attribute, Values> entry : data.entrySet()) {
            // Get the tracker from the column header
            Tracker tracker = entry.getKey().getTracker();

            // Create a new WorkerSubTask
            WorkerSubTask wst = new WorkerSubTask(
                    task.getID(seed),
                    entry.getKey(),
                    entry.getValue(),
                    dbName,
                    path,
                    sourceName
            );
            seed++;

            // Submit the subTask to the conductor
            this.conductor.submitSubTask(wst);
            tracker.submitChunk();
        }
    }

    @Override
    public void run() {

        while(doWork) {
            try {
                // Try to pull a task from the conductor
                task = this.conductor.pullTask();
                if(task == null) {
                    continue;
                }

                // Access attributes and attribute type through first read
                Connector c = task.getConnector();
                PreAnalyzer pa = new PreAnalyzer();
                pa.composeConnector(c);

                LOG.info("TaskProducer: {} processing: {}", workerName, c.getSourceName());

                // Read initial records to figure out attribute types
                Map<Attribute, Values> initData = pa.readRows(numRecordChunk);
                if(initData == null) {
                    LOG.warn("No data read from: {}", c.getSourceName());
                    task.close();
                }

                this.submitSubTasks(initData, c.getDBName(), c.getPath(), c.getSourceName());

                // Consume all remaining records from the connector
                Map<Attribute, Values> data = pa.readRows(numRecordChunk);
                int records = 0;
                while(data != null) {
                    records = records + data.size();
                    Conductor.recordsPerSecond.mark(records);

                    // Submit the subTasks to the conductor
                    this.submitSubTasks(data, c.getDBName(), c.getPath(), c.getSourceName());

                    // Read next chunk of data
                    data = pa.readRows(numRecordChunk);
                }

                // Notify all the trackers that we're done reading
                for (Attribute attribute : initData.keySet()) {
                    attribute.getTracker().finishReading();
                }

                // Close the task
                task.close();
                this.conductor.notifyProcessedTask(initData.size());
            }
            catch(Exception e) {
                String init = "#########";
                String msg = task.toString() +" $FAILED$ cause-> "+ e.getMessage();
                StackTraceElement[] trace = e.getStackTrace();
                StringBuffer sb = new StringBuffer();
                sb.append(init);
                sb.append(System.lineSeparator());
                sb.append(msg);
                sb.append(System.lineSeparator());
                for(int i = 0; i < trace.length; i++) {
                    sb.append(trace[i].toString());
                    sb.append(System.lineSeparator());
                }
                sb.append(System.lineSeparator());
                String log = sb.toString();
                this.conductor.submitError(log);
            }
        }
        LOG.info("THREAD: {} stopping", workerName);
    }
}
