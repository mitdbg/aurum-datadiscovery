package core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analysis.Analysis;
import analysis.AnalyzerFactory;
import analysis.NumericalAnalysis;
import analysis.TextualAnalysis;
import analysis.modules.EntityAnalyzer;
import core.WorkerTask.TaskPackageType;
import core.config.ProfilerConfig;
import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import inputoutput.conn.BenchmarkingConnector;
import inputoutput.conn.BenchmarkingData;
import inputoutput.conn.Connector;
import preanalysis.PreAnalyzer;
import preanalysis.Values;
import store.Store;

import javax.xml.soap.Text;

public class TaskConsumer implements Worker {

	final private Logger LOG = LoggerFactory.getLogger(TaskConsumer.class.getName());
	
	private final int pseudoRandomSeed = 1;
	
	private Conductor conductor;
	private boolean doWork = true;
	private String workerName;
	private WorkerSubTask task;
	private Store store;

	// Benchmark variables
	private boolean first = true;
	private BenchmarkingData benchData;

	public TaskConsumer(Conductor conductor, ProfilerConfig pc, String workerName, Store store) {
		this.conductor = conductor;
		this.store = store;
		this.workerName = workerName;
	}
	
	public void stop() {
		this.doWork = false;
	}

	@Override
	public void run() {

		while (doWork) {
			try {
				task = this.conductor.pullSubTask();
				if (task == null) {
					continue;
				}

				DataIndexer indexer = new FilterAndBatchDataIndexer(store, task.getDBName(), task.getPath(), task.getSourceName());

				// Feed values to analyzers
				for (Entry<Attribute, Values> entry : task.getValues().entrySet()) {
					String atName = entry.getKey().getColumnName();
					Values values = entry.getValue();
					if (values.areFloatValues()) {
						((NumericalAnalysis) entry.getKey().getAnalyzer()).feedFloatData(values.getFloats());
					} else if (values.areIntegerValues()) {
						((NumericalAnalysis) entry.getKey().getAnalyzer()).feedIntegerData(values.getIntegers());
					} else if (values.areStringValues()) {
						((TextualAnalysis) entry.getKey().getAnalyzer()).feedTextData(values.getStrings());
					}
				}

				// Index the values in the store
				indexer.indexData(task.getDBName(), task.getPath(), task.getValues());

				// Get results and wrap them in a Result object
				// FIXME: WorkerTaskResultHolder wtrf = new WorkerTaskResultHolder(c.getSourceName(), c.getAttributes(), analyzers);
//				WorkerTaskResultHolder wtrf = new WorkerTaskResultHolder(task.getDBName(), task.getPath(), task.getSourceName(), c.getAttributes(), analyzers);

//				List<WorkerTaskResult> rs = WorkerTaskResultHolder.makeFakeOne();
//				WorkerTaskResultHolder wtrf = new WorkerTaskResultHolder(rs);

//				List<WorkerTaskResult> results = wtrf.get();

				// Finish the subtask, and store a summary document if that was the last subtask
				task.getTracker().processChunk();
				if (task.getTracker().isDoneProcessing()) {
					// TODO: merge and store document
				}

//				for(WorkerTaskResult wtr : results) {
//					store.storeDocument(wtr);
//				}

//				conductor.notifyProcessedTask(results.size());
			} catch (Exception e) {
				String init = "#########";
				String msg = task.toString() + " $FAILED$ cause-> " + e.getMessage();
				StackTraceElement[] trace = e.getStackTrace();
				StringBuffer sb = new StringBuffer();
				sb.append(init);
				sb.append(System.lineSeparator());
				sb.append(msg);
				sb.append(System.lineSeparator());
				for (int i = 0; i < trace.length; i++) {
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
