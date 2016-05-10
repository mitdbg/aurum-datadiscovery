/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis;

import java.util.Iterator;
import java.util.List;

import analysis.modules.Cardinality;
import analysis.modules.CardinalityAnalyzer;
import analysis.modules.DataType;
import analysis.modules.DataTypeAnalyzer;
import analysis.modules.Entities;
import analysis.modules.EntityAnalyzer;
import analysis.modules.Overlap;
import analysis.modules.OverlapAnalyzer;
import analysis.modules.Range;
import analysis.modules.RangeAnalyzer;
import analysis.modules.Signature;
import analysis.modules.SignatureAnalyzer;
import inputoutput.Value;

public class Analyzer implements Analysis, DataConsumer {
	
	private List<DataConsumer> analyzers;
	private DataTypeAnalyzer dta;
	private CardinalityAnalyzer ca;
	private EntityAnalyzer ea;
	private OverlapAnalyzer oa;
	private RangeAnalyzer ra;
	private SignatureAnalyzer sa;
	
	private Analyzer() {
		// TODO: Get from properties which one of these we want to configure
		dta = new DataTypeAnalyzer();
		ca = new CardinalityAnalyzer();
		ea = new EntityAnalyzer();
		oa = new OverlapAnalyzer();
		ra = new RangeAnalyzer();
		sa = new SignatureAnalyzer();
		analyzers.add(dta);
		analyzers.add(ca);
		analyzers.add(ea);
		analyzers.add(oa);
		analyzers.add(ra);
		analyzers.add(sa);
	}
	
	public static Analyzer makeAnalyzer() {
		return new Analyzer();
	}
	
	@Override
	public <T extends DataType> boolean feedData(List<Value<T>> records) {
		
		Iterator<DataConsumer> dcs = analyzers.iterator();
		while(dcs.hasNext()) {
			DataConsumer dc = dcs.next();
			dc.feedData(records);
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
	public Signature getSignature() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Overlap getOverlap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Cardinality getCardinality() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Range getNumericalRange() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entities getEntities() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
