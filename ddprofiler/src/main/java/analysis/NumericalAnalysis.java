/**
 * 
 */
package analysis;

import analysis.modules.NumericalSignature;
import analysis.modules.Range;
import inputoutput.Attribute.AttributeType;

/**
 * @author Raul - raulcf@csail.mit.edu
 * Sibo (edit)
 */
public interface NumericalAnalysis extends Analysis, IntegerDataConsumer, FloatDataConsumer {

	public Range getNumericalRange(AttributeType type);
	// add an interface to return the quantile
	public long getQuantile(double p);
	public NumericalSignature getSignature();
	
}
