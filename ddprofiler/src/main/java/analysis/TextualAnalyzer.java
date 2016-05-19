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
import analysis.modules.Entities;
import analysis.modules.EntityAnalyzer;
import analysis.modules.Signature;
import analysis.modules.SignatureAnalyzer;

public class TextualAnalyzer implements TextualAnalysis {

	private List<DataConsumer> analyzers;
	private DataTypeAnalyzer dta;
	private CardinalityAnalyzer ca;
	private EntityAnalyzer ea;
	private SignatureAnalyzer sa;
	
	private TextualAnalyzer(EntityAnalyzer ea) {
		analyzers = new ArrayList<>();
		dta = new DataTypeAnalyzer();
		ca = new CardinalityAnalyzer();
		this.ea = ea;
		sa = new SignatureAnalyzer();
		analyzers.add(dta);
		analyzers.add(ca);
		analyzers.add(ea);
		analyzers.add(ea);
		analyzers.add(sa);
	}
	
	public static TextualAnalyzer makeAnalyzer(EntityAnalyzer ea) {
		ea.clear();
		return new TextualAnalyzer(ea);
	}
	
	@Override
	public boolean feedTextData(List<String> records) {
		Iterator<DataConsumer> dcs = analyzers.iterator();
		while(dcs.hasNext()) {
			TextualDataConsumer dc = (TextualDataConsumer) dcs.next();
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
	public DataType getDataType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Signature getSignature() {
		return sa.getSignature();
	}

	@Override
	public Cardinality getCardinality() {
		return ca.getCardinality();
	}

	@Override
	public Entities getEntities() {
		return ea.getEntities();
	}

}
