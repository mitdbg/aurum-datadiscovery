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
import core.TaskPackage.TaskPackageType;
import core.config.ProfilerConfig;
import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import inputoutput.conn.Connector;
import preanalysis.PreAnalyzer;
import preanalysis.Values;
import store.Store;

public class Worker implements Runnable {

	final private Logger LOG = LoggerFactory.getLogger(Worker.class.getName());
	
	private Conductor conductor;
	private boolean doWork = true;
	private String workerName;
	private WorkerTask task;
	private int numRecordChunk;
	private Store store;
	
	private BlockingQueue<TaskPackage> taskQueue;
	private BlockingQueue<ErrorPackage> errorQueue;
	
	// cached object
	private EntityAnalyzer ea;

	public Worker(Conductor conductor, ProfilerConfig pc, String workerName, BlockingQueue<TaskPackage> taskQueue, BlockingQueue<ErrorPackage> errorQueue, Store store, EntityAnalyzer cached) {
		this.conductor = conductor;
		this.numRecordChunk = pc.getInt(ProfilerConfig.NUM_RECORD_READ);
		this.store = store;
		this.ea = cached;
		this.workerName = workerName;
		this.taskQueue = taskQueue;
		this.errorQueue = errorQueue;
	}
	
	public void stop() {
		this.doWork = false;
	}
	
	private WorkerTask pullTask() {
		// Attempt to consume new task
		WorkerTask wt = null;
		try {
			TaskPackage tp = taskQueue.poll(500, TimeUnit.MILLISECONDS);
			if (tp == null) {
				return null;
			}
			// Create real worker task on demand
			if(tp.getType() == TaskPackageType.CSV) {
				wt = WorkerTask.makeWorkerTaskForCSVFile(tp.getPath(), tp.getName(), tp.getSeparator());
			}
			else if(tp.getType() == TaskPackageType.DB) {
				wt = WorkerTask.makeWorkerTaskForDB(tp.getDBType(), tp.getIp(), tp.getPort(), tp.getDBName(), tp.getStr(), tp.getUsername(), tp.getPassword());
			}
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		return wt;
	}

	@Override
	public void run() {
		
		while(doWork) {
			try {
				
				// Collection to hold analyzers
				Map<String, Analysis> analyzers = new HashMap<>();
				
				task = pullTask();
				
				if(task == null) {
					continue;
				}
				
				DataIndexer indexer = new FilterAndBatchDataIndexer(store, task.getConnector().getSourceName());
				
				// Access attributes and attribute type through first read
				Connector c = task.getConnector();
				PreAnalyzer pa = new PreAnalyzer();
				pa.composeConnector(c);
				
				LOG.info("Worker: {} processing: {}", workerName, c.getSourceName());
				
				Map<Attribute, Values> initData = pa.readRows(numRecordChunk);
				if(initData == null) {
					LOG.warn("No data read from: {}", c.getSourceName());
					task.close();
				}
				
				// Read initial records to figure out attribute types etc
				//FIXME: readFirstRecords(initData, analyzers);
				readFirstRecords(initData, analyzers, indexer);
				
				// Consume all remaining records from the connector
				Map<Attribute, Values> data = pa.readRows(numRecordChunk);
				int records = 0;
				while(data != null) {
					indexer.indexData(data);
					records = records + data.size();
					// Do the processing
					// FIXME: feedValuesToAnalyzers(data, analyzers);
					feedValuesToAnalyzers(data, analyzers);
					
					// Read next chunk of data
					data = pa.readRows(numRecordChunk);
				}
				
				// Get results and wrap them in a Result object
				// FIXME: WorkerTaskResultHolder wtrf = new WorkerTaskResultHolder(c.getSourceName(), c.getAttributes(), analyzers);
				WorkerTaskResultHolder wtrf = new WorkerTaskResultHolder(c.getDBName(), c.getSourceName(), c.getAttributes(), analyzers);
				
//				List<WorkerTaskResult> rs = WorkerTaskResultHolder.makeFakeOne();
//				WorkerTaskResultHolder wtrf = new WorkerTaskResultHolder(rs);
				
//				// FIXME: indexer.flushAndClose();
				task.close();
				List<WorkerTaskResult> results = wtrf.get();
				
				for(WorkerTaskResult wtr : results) {
					store.storeDocument(wtr);
				}
				
				conductor.notifyProcessedTask(results.size());
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
				try {
					errorQueue.put(new ErrorPackage(log));
				} 
				catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
		LOG.info("THREAD: {} stopping", workerName);
	}
	
	private void readFirstRecords(Map<Attribute, Values> initData, Map<String, Analysis> analyzers, DataIndexer indexer) {
		for(Entry<Attribute, Values> entry : initData.entrySet()) {
			Attribute a = entry.getKey();
			AttributeType at = a.getColumnType();
			Analysis analysis = null;
			if(at.equals(AttributeType.FLOAT)) {
				analysis = AnalyzerFactory.makeNumericalAnalyzer();
				((NumericalAnalysis)analysis).feedFloatData(entry.getValue().getFloats());
			}
			else if(at.equals(AttributeType.INT)) {
				analysis = AnalyzerFactory.makeNumericalAnalyzer();
				((NumericalAnalysis)analysis).feedIntegerData(entry.getValue().getIntegers());
			}
			else if(at.equals(AttributeType.STRING)) {
				analysis = AnalyzerFactory.makeTextualAnalyzer(ea);
				((TextualAnalysis)analysis).feedTextData(entry.getValue().getStrings());
			}
			analyzers.put(a.getColumnName(), analysis);
		}
		
		// Index text read so far
		indexer.indexData(initData);
	}
	
	private void feedValuesToAnalyzers(Map<Attribute, Values> data, Map<String, Analysis> analyzers) {
		// Do the processing
		for(Entry<Attribute, Values> entry : data.entrySet()) {
			String atName = entry.getKey().getColumnName();
			Values vs = entry.getValue();
			if(vs.areFloatValues()) {
				((NumericalAnalysis)analyzers.get(atName)).feedFloatData(vs.getFloats());
			}
			else if(vs.areIntegerValues()) {
				((NumericalAnalysis)analyzers.get(atName)).feedIntegerData(vs.getIntegers());
			}
			else if(vs.areStringValues()) {
				((TextualAnalysis)analyzers.get(atName)).feedTextData(vs.getStrings());
			}
		}
	}

}
