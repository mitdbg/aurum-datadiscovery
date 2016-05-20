package analysis.modules;

import java.util.List;

import analysis.FloatDataConsumer;
import analysis.IntegerDataConsumer;

public class NumericalSignatureAnalyzer implements IntegerDataConsumer, FloatDataConsumer {

	private NumericalSignature nSig;
	
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

	public NumericalSignature getSignature() {
		return nSig;
	}

}
