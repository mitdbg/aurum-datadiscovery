/**
 * 
 */
package analysis;

import analysis.modules.Entities;
import analysis.modules.TextualSignature;

/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
public interface TextualAnalysis extends Analysis,  TextualDataConsumer {

	public Entities getEntities();
	public TextualSignature getSignature();
}
