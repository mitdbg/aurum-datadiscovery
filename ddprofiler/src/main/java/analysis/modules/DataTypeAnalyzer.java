/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

import java.util.List;

import analysis.FloatDataConsumer;
import analysis.IntegerDataConsumer;
import analysis.TextualDataConsumer;

public class DataTypeAnalyzer implements IntegerDataConsumer, FloatDataConsumer, TextualDataConsumer {

	@Override
	public boolean feedTextData(List<String> records) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean feedFloatData(List<Float> records) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean feedIntegerData(List<Integer> records) {
		// TODO Auto-generated method stub
		return false;
	}

}
