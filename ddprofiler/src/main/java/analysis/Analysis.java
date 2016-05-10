/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis;

import analysis.modules.Cardinality;
import analysis.modules.DataType;
import analysis.modules.Entities;
import analysis.modules.Overlap;
import analysis.modules.Range;
import analysis.modules.Signature;

public interface Analysis {

	public DataProfile getProfile();
	public DataType getDataType();
	public Signature getSignature();
	public Overlap getOverlap();
	public Cardinality getCardinality();
	public Range getNumericalRange();
	public Entities getEntities();
	
}
