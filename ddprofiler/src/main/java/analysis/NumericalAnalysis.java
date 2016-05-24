/**
 * 
 */
package analysis;

import analysis.modules.NumericalSignature;
import analysis.modules.Range;

/**
 * @author Raul - raulcf@csail.mit.edu
 * Sibo (edit)
 */
public interface NumericalAnalysis extends Analysis, IntegerDataConsumer, FloatDataConsumer {

	public Range getNumericalRange();
	// add an interface to return the quantile
	public long getQuantile(double p);
	public NumericalSignature getSignature();
	
}
