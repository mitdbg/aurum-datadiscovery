/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import analysis.TextualDataConsumer;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;

public class EntityAnalyzer implements TextualDataConsumer {

	private Set<String> entities = null;
	private NameFinderME nameFinder = null;
	
	public EntityAnalyzer() {
		TokenNameFinderModel model = loadModel();
		nameFinder = new NameFinderME(model);
		entities = new HashSet<>();
	}
	
	public Entities getEntities() {
		Entities e = new Entities(entities);
		return e;
	}

	@Override
	public boolean feedTextData(List<String> records) {
		// TODO: preprocessing to records?
		String[] sentences = new String[records.size()];
		for(int i = 0; i < sentences.length; i++) {
			sentences[i] = records.get(i);
		}
		Span nameSpans[] = nameFinder.find(sentences);
		
		for(Span s : nameSpans) {
			entities.add(s.getType());
		}
		
		// This is supposed to be called temporarily to clean up data
		nameFinder.clearAdaptiveData();
		
		return false;
	}
	
	public TokenNameFinderModel loadModel() {
		// TODO: what model/models do we need to load?
		InputStream modelIn = null;
		TokenNameFinderModel model = null;
		
		try {
			modelIn = new FileInputStream("en-ner-person.bin");
			model = new TokenNameFinderModel(modelIn);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (modelIn != null) {
				try {
					modelIn.close();
				}
				catch (IOException e) {
				}
			}
		}
		return model;
	}
}
