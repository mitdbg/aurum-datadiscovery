package core;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import analysis.Analysis;
import analysis.AnalyzerFactory;
import analysis.NumericalAnalysis;
import analysis.TextualAnalysis;
import core.config.ProfilerConfig;
import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import inputoutput.conn.Connector;
import preanalysis.PreAnalyzer;
import preanalysis.Values;

public class Worker implements Callable<WorkerTaskResultFuture> {

	private WorkerTask task;
	private int numRecordChunk;
	
	public Worker(WorkerTask task, ProfilerConfig pc) {
		this.task = task;
		numRecordChunk = pc.getInt(ProfilerConfig.NUM_RECORD_READ);
	}

	@Override
	public WorkerTaskResultFuture call() throws Exception {
		// Collection to hold analyzers
		Map<String, Analysis> analyzers = new HashMap<>();
		
		// Access attributes and attribute type through first read
		Connector c = task.getConnector();
		PreAnalyzer pa = new PreAnalyzer();
		pa.composeConnector(c);
		
		Map<Attribute, Values> initData = pa.readRows(numRecordChunk);
		for(Entry<Attribute, Values> entry : initData.entrySet()) {
			Attribute a = entry.getKey();
			AttributeType at = a.getColumnType();
			Analysis analysis = null;
			if(at.equals(AttributeType.FLOAT)) {
				analysis = AnalyzerFactory.makeNumericalAnalyzer();
				((NumericalAnalysis)analysis).feedFloatData(entry.getValue().getFloats());
			}
			else if(at.equals(AttributeType.STRING)) {
				analysis = AnalyzerFactory.makeTextualAnalyzer();
				((TextualAnalysis)analysis).feedTextData(entry.getValue().getStrings());
			}
			analyzers.put(a.getColumnName(), analysis);
		}
		
		// Consume all records from the connector
		Map<Attribute, Values> data = pa.readRows(numRecordChunk);
		while(data != null) {
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
			data = pa.readRows(numRecordChunk);
		}
		
		// Get results and wrap them in a Result object
		WorkerTaskResultFuture wtrf = new WorkerTaskResultFuture(c.getSourceName(), c.getAttributes(), analyzers);
		
		return wtrf;
	}

}
