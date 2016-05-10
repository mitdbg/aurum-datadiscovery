/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis;

import java.util.List;

import analysis.modules.DataType;
import inputoutput.Value;

public interface DataConsumer {

	public <T extends DataType> boolean feedData(List<Value<T>> records);
}
