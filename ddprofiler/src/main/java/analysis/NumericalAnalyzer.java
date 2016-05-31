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
import analysis.modules.DataType;
import analysis.modules.DataTypeAnalyzer;
import analysis.modules.NumericalSignature;
import analysis.modules.NumericalSignatureAnalyzer;
import analysis.modules.Range;
import analysis.modules.RangeAnalyzer;


public class NumericalAnalyzer implements NumericalAnalysis {
	
	private List<DataConsumer> analyzers;
	private DataTypeAnalyzer dta;
	private CardinalityAnalyzer ca;
	private RangeAnalyzer ra;
	private NumericalSignatureAnalyzer sa;
	
	
	private NumericalAnalyzer() {
		analyzers = new ArrayList<>();
		dta = new DataTypeAnalyzer();
		ca = new CardinalityAnalyzer();
		ra = new RangeAnalyzer();
		sa = new NumericalSignatureAnalyzer();
		analyzers.add(dta);
		analyzers.add(ca);
		analyzers.add(ra);
		analyzers.add(sa);
	}
	
	public static NumericalAnalyzer makeAnalyzer() {
		return new NumericalAnalyzer();
	}
	
	@Override
	public boolean feedIntegerData(List<Integer> records) {
		
		Iterator<DataConsumer> dcs = analyzers.iterator();
		while(dcs.hasNext()) {
			IntegerDataConsumer dc = (IntegerDataConsumer) dcs.next();
			dc.feedIntegerData(records);
		}
		
		return false;
	}
	
	@Override
	public boolean feedFloatData(List<Float> records) {
		
		Iterator<DataConsumer> dcs = analyzers.iterator();
		while(dcs.hasNext()) {
			FloatDataConsumer dc = (FloatDataConsumer) dcs.next();
			dc.feedFloatData(records);
		}
		
		return false;
	}
	
	@Override
	public DataProfile getProfile() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataType getDataType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumericalSignature getSignature() {
		return sa.getSignature();
	}

	@Override
	public Cardinality getCardinality() {
		return ca.getCardinality();
	}

	@Override
	public Range getNumericalRange() {
		return ra.getFloatRange();
	}

	@Override
	public long getQuantile(double p) {
		return ra.getQuantile(p);
	}
	
}
