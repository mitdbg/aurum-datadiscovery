/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

import java.util.List;

import analysis.FloatDataConsumer;
import analysis.IntegerDataConsumer;

public class RangeAnalyzer implements IntegerDataConsumer, FloatDataConsumer {

	private int totalRecords;
	private int max;
	private int min;
	private double avg;


	@Override
	public boolean feedIntegerData(List<Integer> records) {
		
		for(int value : records) {
			totalRecords++;
				
		}
		
		return true;
	}
	
	@Override
	public boolean feedFloatData(List<Float> records) {
		// TODO Auto-generated method stub
		return false;
	}

}
