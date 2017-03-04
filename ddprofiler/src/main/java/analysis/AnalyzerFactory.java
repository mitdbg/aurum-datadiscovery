package analysis;

import analysis.modules.EntityAnalyzer;

public class AnalyzerFactory {

  public static NumericalAnalysis makeNumericalAnalyzer() {
    NumericalAnalyzer na = NumericalAnalyzer.makeAnalyzer();
    return na;
  }

  public static TextualAnalysis makeTextualAnalyzer(int pseudoRandomSeed) {
    TextualAnalyzer ta = TextualAnalyzer.makeAnalyzer(pseudoRandomSeed);
    return ta;
  }
}
