package analysis.modules;

import java.util.List;

import analysis.TextualDataConsumer;

public class TextualSignatureAnalyzer implements TextualDataConsumer {

	private TextualSignature tSig;
	
	@Override
	public boolean feedTextData(List<String> records) {
		// TODO Auto-generated method stub
		return false;
	}

	public TextualSignature getSignature() {
		return tSig;
	}


}
