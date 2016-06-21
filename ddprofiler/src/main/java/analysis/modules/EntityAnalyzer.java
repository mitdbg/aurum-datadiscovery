/**
 * @author Raul - raulcf@csail.mit.edu
 * @author Sibo Wang (edit)
 */
package analysis.modules;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import analysis.AnalyzerFactory;
import analysis.TextualDataConsumer;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;

public class EntityAnalyzer implements TextualDataConsumer {

	private Set<String> entities = null;
	private static List<NameFinderME> nameFinderList = null;
	private static List<TokenNameFinderModel> modelList = null;

	static{
		//System.out.println("we should print this initialization message only once");
		InputStream nlpModeConfigStream;
		try {
			nlpModeConfigStream = AnalyzerFactory.class.getClassLoader().getResource(
					"config" + File.separator + "nlpmodel.config").openStream();
			modelList = loadModel(nlpModeConfigStream);
			
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		//System.out.println("finished the first time initializing");
	}
	
	
	public EntityAnalyzer(List<TokenNameFinderModel> modelList) {
		entities = new HashSet<>();
		nameFinderList = new Vector<NameFinderME>();
		
		for (TokenNameFinderModel tfModel : modelList) {
			NameFinderME nameFinder = new NameFinderME(tfModel);
			nameFinderList.add(nameFinder);
		}
	}
	
	public EntityAnalyzer() {
		entities = new HashSet<>();
		nameFinderList = new Vector<NameFinderME>();
		for (TokenNameFinderModel tfModel : modelList) {
			NameFinderME nameFinder = new NameFinderME(tfModel);
			nameFinderList.add(nameFinder);
		}
	}
	
	public List<TokenNameFinderModel> getCachedModelList() {
		return modelList;
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

	
	
	public static List<TokenNameFinderModel> loadModel(InputStream modelConfigStream) {
		// TODO: what model/models do we need to load?
		/*
		 * currently, we adopted the models provided by openNLP the detailed
		 * models are listed in the model_list_file_name
		 */
		List<TokenNameFinderModel> modelList = new Vector<TokenNameFinderModel>();
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = modelConfigStream;
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
				} 
				catch (IOException e) {
					e.printStackTrace();
				} 
				finally {
					if (modelIn != null) {
						try {
							modelIn.close();
						} catch (IOException e) {
						}
					}
				}
			}
		} 
		catch (IOException e1) {
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
