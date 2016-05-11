/**
 * 
 */
package analysis;

import analysis.modules.Entities;

/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
public interface TextualAnalysis extends Analysis {

	public Entities getEntities();
}
