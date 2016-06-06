package core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import analysis.Analysis;
import analysis.NumericalAnalysis;
import analysis.TextualAnalysis;
import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;

public class WorkerTaskResultHolder {

	private List<WorkerTaskResult> results;
	
	public WorkerTaskResultHolder(String sourceName, List<Attribute> attributes, Map<String, Analysis> analyzers) {
		List<WorkerTaskResult> rs = new ArrayList<>();
		for(Attribute a : attributes) {
			AttributeType at = a.getColumnType();
			Analysis an = analyzers.get(a.getColumnName());
			int id = Utils.computeAttrId(sourceName, a.getColumnName());
			if(at.equals(AttributeType.FLOAT)) {
				NumericalAnalysis na = ((NumericalAnalysis)an);
				WorkerTaskResult wtr = new WorkerTaskResult(
						id,
						sourceName,
						a.getColumnName(),
						"N",
						(int)na.getCardinality().getTotalRecords(),
						(int)na.getCardinality().getUniqueElements(),
						na.getNumericalRange(AttributeType.FLOAT).getMinF(),
						na.getNumericalRange(AttributeType.FLOAT).getMaxF(),
						na.getNumericalRange(AttributeType.FLOAT).getAvg());
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
						na.getNumericalRange(AttributeType.INT).getMin(),
						na.getNumericalRange(AttributeType.INT).getMax(),
						na.getNumericalRange(AttributeType.INT).getAvg());
				rs.add(wtr);
			}
			else if(at.equals(AttributeType.STRING)) {
				TextualAnalysis ta = ((TextualAnalysis)an);
				List<String> ents = ta.getEntities().getEntities();
				StringBuffer sb = new StringBuffer();
				for(String str : ents) {
					sb.append(str);
					sb.append(" ");
				}
				String entities = sb.toString();
				
				WorkerTaskResult wtr = new WorkerTaskResult(
						id,
						sourceName,
						a.getColumnName(),
						"T",
						(int)ta.getCardinality().getTotalRecords(),
						(int)ta.getCardinality().getUniqueElements(),
						entities);
				rs.add(wtr);
			}
		}
		this.results = rs;
	}
	
	public List<WorkerTaskResult> get() {
		return results;
	}

}
