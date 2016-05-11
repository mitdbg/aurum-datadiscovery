package analysis;


public class AnalyzerFactory {

	public static NumericalAnalysis makeNumericalAnalyzer() {
		NumericalAnalyzer na = NumericalAnalyzer.makeAnalyzer();
		return na;
	}
	
	public static TextualAnalysis makeTextualAnalyzer() {
		TextualAnalyzer ta = TextualAnalyzer.makeAnalyzer();
		return ta;
	}
	
}
