/**
 * 
 */
package analysis;

import analysis.modules.Range;

/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
public interface NumericalAnalysis extends Analysis {

	public Range getNumericalRange();
	
}
