package core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import analysis.Analysis;
import analysis.AnalyzerFactory;
import core.config.ProfilerConfig;
import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import inputoutput.conn.Connector;
import preanalysis.PreAnalyzer;

public class Worker implements Callable<WorkerTaskResult> {

	private WorkerTask task;
	private int numRecordChunk;
	
	public Worker(WorkerTask task, ProfilerConfig pc) {
		this.task = task;
		numRecordChunk = pc.getInt(ProfilerConfig.NUM_RECORD_READ);
	}

	@Override
	public WorkerTaskResult call() throws Exception {
		// Collection to hold analyzers
		Map<String, Analysis> analyzers = new HashMap<>();
		
		// Access attributes and attribute type through first read
		Connector c = task.getConnector();
		PreAnalyzer pa = new PreAnalyzer();
		pa.composeConnector(c);
		
		
		Map<Attribute, List<Object>> data = pa.readRows(numRecordChunk);
		for(Entry<Attribute, List<Object>> entry : data.entrySet()) {
			Attribute a = entry.getKey();
			AttributeType at = a.getColumnType();
			Analysis analysis = null;
			if(at.equals(AttributeType.FLOAT)) {
				analysis = AnalyzerFactory.makeNumericalAnalyzer();
				//((NumericalAnalysis)analysis).feedFloatData(entry.getValue());
			}
			else if(at.equals(AttributeType.STRING)) {
				analysis = AnalyzerFactory.makeTextualAnalyzer();
			}
			analyzers.put(a.getColumnName(), analysis);
		}
		
		return null;
	}

}
