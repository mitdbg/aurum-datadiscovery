/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

import java.util.List;

import analysis.DataConsumer;
import inputoutput.Value;

public class RangeAnalyzer implements DataConsumer {

	private int totalRecords;
	private int max;
	private int min;
	private double avg;
	
	@Override
	public <T extends DataType> boolean feedData(List<Value<T>> records) {
		
		DataType.Type t = ((DataType)records.get(0).v).getType();
		
		if(t == DataType.Type.INT) {
			for(Value<T> value : records) {
				totalRecords++;
				
			}
		}
		
		return true;
	}

}
