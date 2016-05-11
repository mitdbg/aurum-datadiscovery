package analysis;


public class AnalyzerFactory {

	public NumericalAnalysis makeNumericalAnalyzer() {
		NumericalAnalyzer na = NumericalAnalyzer.makeAnalyzer();
		return na;
	}
	
	public TextualAnalysis makeTextualAnalyzer() {
		TextualAnalyzer ta = TextualAnalyzer.makeAnalyzer();
		return ta;
	}
	
}
