package core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import analysis.Analysis;
import analysis.NumericalAnalysis;
import analysis.TextualAnalysis;
import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;

public class WorkerTaskResultFuture implements Future<List<WorkerTaskResult>> {

	private boolean isDone = false;
	private boolean isCancelled = false;
	private List<WorkerTaskResult> results;
	
	public WorkerTaskResultFuture(String sourceName, List<Attribute> attributes, Map<String, Analysis> analyzers) {
		List<WorkerTaskResult> rs = new ArrayList<>();
		for(Attribute a : attributes) {
			AttributeType at = a.getColumnType();
			Analysis an = analyzers.get(a.getColumnName());
			int id = computeAttrId(sourceName, a.getColumnName());
			if(at.equals(AttributeType.FLOAT)) {
				NumericalAnalysis na = ((NumericalAnalysis)an);
				WorkerTaskResult wtr = new WorkerTaskResult(
						id,
						sourceName,
						a.getColumnName(),
						"N",
						(int)na.getCardinality().getTotalRecords(),
						(int)na.getCardinality().getUniqueElements(),
						na.getNumericalRange().getMinF(),
						na.getNumericalRange().getMaxF(),
						na.getNumericalRange().getAvg());
				rs.add(wtr);
			}
			else if(at.equals(AttributeType.INT)) {
				NumericalAnalysis na = ((NumericalAnalysis)an);
				WorkerTaskResult wtr = new WorkerTaskResult(
						id,
						sourceName,
						a.getColumnName(),
						"N",
						(int)na.getCardinality().getTotalRecords(),
						(int)na.getCardinality().getUniqueElements(),
						na.getNumericalRange().getMin(),
						na.getNumericalRange().getMax(),
						na.getNumericalRange().getAvg());
				rs.add(wtr);
			}
			else if(at.equals(AttributeType.STRING)) {
				TextualAnalysis ta = ((TextualAnalysis)an);
				WorkerTaskResult wtr = new WorkerTaskResult(
						id,
						sourceName,
						a.getColumnName(),
						"T",
						(int)ta.getCardinality().getTotalRecords(),
						(int)ta.getCardinality().getUniqueElements(),
						(String[]) ta.getEntities().getEntities().toArray());
				rs.add(wtr);
			}
		}
		isDone = true;
	}
	
	private int computeAttrId(String sourceName, String columnName) {
		String t = sourceName.concat(columnName);
		return t.hashCode();
	}

	@Override
	public boolean isCancelled() {
		return isCancelled;
	}

	@Override
	public boolean isDone() {
		return isDone;
	}

	@Override
	public List<WorkerTaskResult> get() throws InterruptedException, ExecutionException {
		return results;
	}

	@Override
	public List<WorkerTaskResult> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		// TODO Auto-generated method stub
		return false;
	}

}
