/**
 * @author Sibo Wang
 *
 */
package analysis.modules;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import analysis.TextualDataConsumer;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;

public class MultiEntityAnalyzer implements TextualDataConsumer {

	private Set<String> entities = null;
	private List<NameFinderME> nameFinderList = null;
	private Properties prop = null;

	public MultiEntityAnalyzer(String NLPModeConfigFileName) {
		prop = new Properties(); 
		entities = new HashSet<>();
		nameFinderList = new Vector<NameFinderME>();
		List<TokenNameFinderModel> modelList = loadModel(NLPModeConfigFileName);
		System.out.println(modelList.size());
		for (TokenNameFinderModel tf_model : modelList) {
			NameFinderME nameFinder = new NameFinderME(tf_model);
			nameFinderList.add(nameFinder);
		}
	}

	public Entities getEntities() {
		Entities e = new Entities(entities);
		return e;
	}

	@Override
	public boolean feedTextData(List<String> records) {
		// TODO: preprocessing to records?
		String[] sentences = new String[records.size()];
		for (int i = 0; i < sentences.length; i++) {
			sentences[i] = records.get(i);
		}
		for (NameFinderME nameFinder : nameFinderList) {
			Span nameSpans[] = nameFinder.find(sentences);

			for (Span s : nameSpans) {
				entities.add(s.getType());
			}

			// This is supposed to be called temporarily to clean up data
			nameFinder.clearAdaptiveData();
		}

		return false;
	}

	public List<TokenNameFinderModel> loadModel(String modelConfigName) {
		// TODO: what model/models do we need to load?
		/*
		 * currently, we adopted the models provided by openNLP the detailed
		 * models are listed in the model_list_file_name
		 */
		List<TokenNameFinderModel> modelList = new Vector<TokenNameFinderModel>();
		InputStream input = null;
		try {
			input = new FileInputStream(modelConfigName);
			prop.load(input);
			Enumeration<?> enumVar = prop.propertyNames();
			while (enumVar.hasMoreElements()) {
				String key = (String) enumVar.nextElement();
				String value = prop.getProperty(key);
				//System.out.println("Key : " + key + ", Value : " + value);

				InputStream modelIn = null;
				TokenNameFinderModel model = null;
				try {
					modelIn = new FileInputStream(value);
					model = new TokenNameFinderModel(modelIn);
					modelList.add(model);
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (modelIn != null) {
						try {
							modelIn.close();
						} catch (IOException e) {
						}
					}
				}
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return modelList;

	}

	public void clear() {
		this.entities.clear();
		for (NameFinderME nameFinder : nameFinderList) {
			nameFinder.clearAdaptiveData();
		}
	}
}
