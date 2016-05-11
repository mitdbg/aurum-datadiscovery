/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis;

import java.util.List;

import analysis.modules.Cardinality;
import analysis.modules.CardinalityAnalyzer;
import analysis.modules.DataType;
import analysis.modules.DataTypeAnalyzer;
import analysis.modules.Entities;
import analysis.modules.EntityAnalyzer;
import analysis.modules.Overlap;
import analysis.modules.OverlapAnalyzer;
import analysis.modules.Signature;
import analysis.modules.SignatureAnalyzer;

public class TextualAnalyzer implements TextualAnalysis, TextualDataConsumer {

	private List<DataConsumer> analyzers;
	private DataTypeAnalyzer dta;
	private CardinalityAnalyzer ca;
	private OverlapAnalyzer oa;
	private EntityAnalyzer ea;
	private SignatureAnalyzer sa;
	
	private TextualAnalyzer() {
		dta = new DataTypeAnalyzer();
		ca = new CardinalityAnalyzer();
		ea = new EntityAnalyzer();
		oa = new OverlapAnalyzer();
		sa = new SignatureAnalyzer();
		analyzers.add(dta);
		analyzers.add(ca);
		analyzers.add(ea);
		analyzers.add(oa);
		analyzers.add(ea);
		analyzers.add(sa);
	}
	
	public static TextualAnalyzer makeAnalyzer() {
		return new TextualAnalyzer();
	}
	
	@Override
	public boolean feedTextData(List<String> records) {
		// TODO Auto-generated method stub
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
	public Entities getEntities() {
		// TODO Auto-generated method stub
		return null;
	}

}
