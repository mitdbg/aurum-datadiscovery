/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import analysis.modules.Cardinality;
import analysis.modules.CardinalityAnalyzer;
import analysis.modules.Entities;
import analysis.modules.EntityAnalyzer;
import analysis.modules.KMinHash;

public class TextualAnalyzer implements TextualAnalysis {

  private static final EntityAnalyzer ea = new EntityAnalyzer();

  private List<DataConsumer> analyzers;
  private CardinalityAnalyzer ca;
  private KMinHash mh;

  private TextualAnalyzer(int pseudoRandomSeed) {
    analyzers = new ArrayList<>();
    mh = new KMinHash(pseudoRandomSeed);
    ca = new CardinalityAnalyzer();
    analyzers.add(ca);
    analyzers.add(mh);
    analyzers.add(ea);
  }

  public static TextualAnalyzer makeAnalyzer(int pseudoRandomSeed) {
    ea.clear();
    return new TextualAnalyzer(pseudoRandomSeed);
  }

  @Override
  synchronized public boolean feedTextData(List<String> records) {
    Iterator<DataConsumer> dcs = analyzers.iterator();
    while (dcs.hasNext()) {
      TextualDataConsumer dc = (TextualDataConsumer)dcs.next();
      dc.feedTextData(records);
    }

    return false;
  }

  @Override
  public DataProfile getProfile() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Cardinality getCardinality() {
    return ca.getCardinality();
  }

  @Override
  public Entities getEntities() {
    return ea.getEntities();
  }
  
  @Override
  public long[] getMH() {
	  return mh.getMH();
  }
  
}
