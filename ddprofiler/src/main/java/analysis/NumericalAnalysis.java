/**
 * 
 */
package analysis;

import analysis.modules.NumericalSignature;
import analysis.modules.Range;

/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
public interface NumericalAnalysis extends Analysis, IntegerDataConsumer, FloatDataConsumer {

	public Range getNumericalRange();
	public NumericalSignature getSignature();
	
}
