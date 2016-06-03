package analysis;

import analysis.modules.EntityAnalyzer;

public class AnalyzerFactory {
	
	public static NumericalAnalysis makeNumericalAnalyzer() {
		NumericalAnalyzer na = NumericalAnalyzer.makeAnalyzer();
		return na;
	}
	
	public static TextualAnalysis makeTextualAnalyzer(EntityAnalyzer ea) {
		TextualAnalyzer ta = TextualAnalyzer.makeAnalyzer(ea);
		return ta;
	}
	
	public static TextualAnalysis makeTextualAnalyzer() {
		EntityAnalyzer ea = new EntityAnalyzer();
		TextualAnalyzer ta = TextualAnalyzer.makeAnalyzer(ea);
		return ta;
	}
}
