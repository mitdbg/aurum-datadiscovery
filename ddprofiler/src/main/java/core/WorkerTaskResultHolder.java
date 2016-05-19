package core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import analysis.Analysis;
import analysis.NumericalAnalysis;
import analysis.TextualAnalysis;
import analysis.modules.Entities;
import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;

public class WorkerTaskResultHolder {

	private List<WorkerTaskResult> results;
	
	public WorkerTaskResultHolder(String sourceName, List<Attribute> attributes, Map<String, Analysis> analyzers) {
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
				List<String> ents = ta.getEntities().getEntities();
				String[] entities = new String[ents.size()];
				for(int i = 0; i < entities.length; i++) {
					entities[i] = ents.get(i);
				}
				
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
	
	private int computeAttrId(String sourceName, String columnName) {
		String t = sourceName.concat(columnName);
		return t.hashCode();
	}
	
	public List<WorkerTaskResult> get() {
		return results;
	}

}
