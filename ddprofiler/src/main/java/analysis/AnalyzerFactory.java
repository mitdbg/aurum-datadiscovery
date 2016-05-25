package analysis;

import analysis.modules.EntityAnalyzer;
import analysis.modules.MultiEntityAnalyzer;

public class AnalyzerFactory {

	// We statically initialize this guy, as it's expensive (it has to load models)

	//private static EntityAnalyzer ea = new EntityAnalyzer();
	private static MultiEntityAnalyzer ea = new MultiEntityAnalyzer(".\\src\\main\\java\\config\\nlpmodel.config");

	
	public static NumericalAnalysis makeNumericalAnalyzer() {
		NumericalAnalyzer na = NumericalAnalyzer.makeAnalyzer();
		return na;
	}
	
	public static TextualAnalysis makeTextualAnalyzer() {
		TextualAnalyzer ta = TextualAnalyzer.makeAnalyzer(ea);
		return ta;
	}
	
}
