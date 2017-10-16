package core;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analysis.Analysis;
import analysis.NumericalAnalysis;
import analysis.TextualAnalysis;
import core.config.ProfilerConfig;
import inputoutput.conn.BenchmarkingData;
import inputoutput.Tracker;
import preanalysis.Values;
import store.Store;

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

	private void processFailedTracker(Tracker tracker) {
		tracker.processChunk();
		if (tracker.isDoneProcessing()) {
			conductor.notifyProcessedColumn();
		}
		conductor.notifyProcessedSubTask();
	}

	@Override
	public void run() {

		while (doWork) {
			try {
				task = this.conductor.pullSubTask();
				if (task == null) {
					continue;
				}

                                // Continue if another subtask of this task has already failed
                                Tracker tracker = task.getTracker();
                                if (tracker.isFailed()) {
        			 	processFailedTracker(tracker);
	                        	continue;
                                }

				DataIndexer indexer = new FilterAndBatchDataIndexer(store, task.getDBName(), task.getPath(), task.getSourceName());

				// Feed values to the analyzer
				Analysis analyzer = task.getAttribute().getAnalyzer();
				Values values = task.getValues();
				if (values.areFloatValues()) {
					((NumericalAnalysis) analyzer).feedFloatData(values.getFloats());
				} else if (values.areIntegerValues()) {
					((NumericalAnalysis) analyzer).feedIntegerData(values.getIntegers());
				} else if (values.areStringValues()) {
					((TextualAnalysis) analyzer).feedTextData(values.getStrings());
				}

				// Index the values in the store
				indexer.indexData(task.getDBName(), task.getPath(), task.getAttribute(), task.getValues());

				// Get results and wrap them in a Result object
				// List<WorkerTaskResult> rs = WorkerTaskResultHolder.makeFakeOne();
				// WorkerTaskResultHolder wtrf = new WorkerTaskResultHolder(rs);

				// List<WorkerTaskResult> results = wtrf.get();

				// Finish the subtask, and store a summary document if that was the last subtask
				tracker.processChunk();
				if (tracker.isDoneProcessing()) {
					WorkerTaskResultHolder wtrf = new WorkerTaskResultHolder(
							task.getDBName(),
							task.getPath(),
							task.getSourceName(),
							Arrays.asList(task.getAttribute())
					);
					List<WorkerTaskResult> results = wtrf.get();
					for(WorkerTaskResult wtr : results) {
						store.storeDocument(wtr);
					}
					conductor.notifyProcessedColumn();
				}

				// FIXME: indexer.flushAndClose();

				conductor.notifyProcessedSubTask();
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
				processFailedTracker(task.getTracker());
			}
		}
		LOG.info("THREAD: {} stopping", workerName);
	}
}
