package core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import analysis.Analysis;
import analysis.AnalyzerFactory;
import analysis.NumericalAnalysis;
import analysis.TextualAnalysis;
import analysis.modules.EntityAnalyzer;
import core.config.ProfilerConfig;
import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import inputoutput.conn.Connector;
import preanalysis.PreAnalyzer;
import preanalysis.Values;
import store.Store;

public class Worker implements Callable<List<WorkerTaskResult>> {

	private WorkerTask task;
	private int numRecordChunk;
	private Store store;
	private Map<String, EntityAnalyzer> cachedEntityAnalyzers;
	
	// cached object
	private EntityAnalyzer ea;
	
	public Worker(WorkerTask task, Store store, ProfilerConfig pc, Map<String, EntityAnalyzer> cachedEntityAnalyzers) {
		this.task = task;
		this.numRecordChunk = pc.getInt(ProfilerConfig.NUM_RECORD_READ);
		this.store = store;
		this.cachedEntityAnalyzers = cachedEntityAnalyzers;
	}

	@Override
	public List<WorkerTaskResult> call() throws Exception {
		// Get thread name and configure entityAnalyzer
		String threadName = Thread.currentThread().getName();
		this.ea = cachedEntityAnalyzers.get(threadName);
		
		// Collection to hold analyzers
		Map<String, Analysis> analyzers = new HashMap<>();
		
		// Access attributes and attribute type through first read
		Connector c = task.getConnector();
		PreAnalyzer pa = new PreAnalyzer();
		pa.composeConnector(c);
		
		Map<Attribute, Values> initData = pa.readRows(numRecordChunk);
		if(initData == null) {
			task.close();
			return null;
		}
		// Read initial records to figure out attribute types etc
		readFirstRecords(initData, analyzers);
		
		// Consume all remaining records from the connector
		Map<Attribute, Values> data = pa.readRows(numRecordChunk);
		int records = 0;
		while(data != null) {
			indexText(data);
			records = records + data.size();
			// Do the processing
			feedValuesToAnalyzers(data, analyzers);
			
			// Read next chunk of data
			data = pa.readRows(numRecordChunk);
		}
		
		// Get results and wrap them in a Result object
		WorkerTaskResultHolder wtrf = new WorkerTaskResultHolder(c.getSourceName(), c.getAttributes(), analyzers);
		
		task.close();
		return wtrf.get();
	}
	
	private void indexText(Map<Attribute, Values> data) {
		for(Entry<Attribute, Values> entry : data.entrySet()) {
			Attribute a = entry.getKey();
			AttributeType at = a.getColumnType();
			
			if(at.equals(AttributeType.STRING)) {
				String sourceName = task.getConnector().getSourceName();
				String columnName = a.getColumnName();
				int id = Utils.computeAttrId(sourceName, columnName);
				store.indexData(id, sourceName, columnName, entry.getValue().getStrings());
			}
		}
	}
	
	private void readFirstRecords(Map<Attribute, Values> initData, Map<String, Analysis> analyzers) {
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
		indexText(initData);
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
