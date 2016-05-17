package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import analysis.AnalyzerFactory;
import analysis.NumericalAnalysis;
import analysis.TextualAnalysis;
import analysis.modules.Cardinality;
import analysis.modules.Entities;
import analysis.modules.Range;
import analysis.modules.Signature;
import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import inputoutput.conn.FileConnector;
import preanalysis.PreAnalyzer;

public class AnalyzerTest {

	private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
	private String filename = "short_cis_course_catalog.csv";
	private String separator = ",";
	private int numRecords = 100;
	
	@Test
	public void test() throws IOException {
		
		FileConnector fc = new FileConnector(path, filename, separator);
		PreAnalyzer pa = new PreAnalyzer();
		pa.composeConnector(fc);
		
		Map<Attribute, List<Object>> data = pa.readRows(numRecords);
		for(Entry<Attribute, List<Object>> a : data.entrySet()) {
			System.out.println("CName: "+a.getKey().getColumnName());
			System.out.println("CType: "+a.getKey().getColumnType());
			AttributeType at = a.getKey().getColumnType();
			if(at.equals(AttributeType.FLOAT)) {
				NumericalAnalysis na = AnalyzerFactory.makeNumericalAnalyzer();
				List<Float> floats = new ArrayList<>();
				for(Object s : a.getValue()) {
					floats.add((float)s);
				}

				na.feedFloatData(floats);
				Cardinality c = na.getCardinality();
				Range r = na.getNumericalRange();
				Signature s = na.getSignature();
				System.out.println("Cardinality:");
				System.out.println(c);
				System.out.println("Range:");
				System.out.println(r);
				System.out.println("Signature:");
				System.out.println(s);
			}
			if(at.equals(AttributeType.STRING)) {
				TextualAnalysis ta = AnalyzerFactory.makeTextualAnalyzer();
				List<String> strs = new ArrayList<>();
				for(Object s : a.getValue()) {
					strs.add((String)s);
				}

				ta.feedTextData(strs);
				Cardinality c = ta.getCardinality();
				Entities e = ta.getEntities();
				Signature s = ta.getSignature();
				System.out.println("Cardinality:");
				System.out.println(c);
				System.out.println("Entities:");
				System.out.println(e);
				System.out.println("Signature:");
				System.out.println(s);
			}
		}
		
	}

}
