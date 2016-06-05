package test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Vector;

import org.junit.Test;

import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import inputoutput.conn.FileConnector;
import preanalysis.PreAnalyzer;
import preanalysis.Values;

public class PreAnalyzerTest {

	private String path = "C:\\";
	private String filename = "Leading_Causes_of_Death__1990-2010.csv";
	//private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
	//private String filename = "short_cis_course_catalog.csv";
	private String separator = ",";
	private int numRecords = 10;
	
	@Test
	public void testPreAnalyzerForTypes() throws IOException {
				
		FileConnector fc = new FileConnector(path, filename, separator);
		
		PreAnalyzer pa = new PreAnalyzer();
		pa.composeConnector(fc);
		
		pa.readRows(numRecords);
		
		List<Attribute> attrs = pa.getEstimatedDataTypes();
		for(Attribute a : attrs) {
			System.out.println(a);
		}
		
		Map<Attribute, Values> data = pa.readRows(numRecords);
		for(Entry<Attribute, Values> a : data.entrySet()) {
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println("NAME: " + a.getKey().getColumnName());
			System.out.println("TYPE: " + a.getKey().getColumnType());
			if(a.getKey().getColumnType().equals(AttributeType.FLOAT)) {
				List<Float> objs = a.getValue().getFloats();
				for(float f : objs) {
					System.out.println(f);
				}
			}
			if(a.getKey().getColumnType().equals(AttributeType.STRING)) {
				List<String> objs = a.getValue().getStrings();
				for(String f : objs) {
					System.out.println(f);
				}
			}
		}
	}
	
	
	public void workloadTest(List<String> test_strings, PreAnalyzer pa){
		long startTime = System.currentTimeMillis();
		for(int i=0; i<test_strings.size(); i++){
			pa.isNumbericalExpcetion(test_strings.get(i));
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Exception method took: " + (endTime - startTime) + " milliseconds");
		System.out.println("----------------------------------------------------------------\n\n");

		System.out.println("Using Reg Exp based solution with workloads that are all numerical values");
		startTime = System.currentTimeMillis();
		for(int i=0; i<test_strings.size(); i++){
			pa.isNumberical(test_strings.get(i));
		}
		endTime = System.currentTimeMillis();
		System.out.println("Reg Exp based method took: " + (endTime - startTime) + " milliseconds");

	}
	
	@Test
	public void testRegExpPerformance(){
		PreAnalyzer pa = new PreAnalyzer();
		final int NUM_TEST_STRINGS=1000000;
		final double DOUBLE_RANGLE_MIN=1.0;
		final double DOUBLE_RANGLE_MAX=10000000.0;
		List<String> testStrings = new Vector<String>();
		double start = DOUBLE_RANGLE_MIN;
		double end = DOUBLE_RANGLE_MAX;
		Random randomSeeds = new Random();

		for(int i=0; i<NUM_TEST_STRINGS; i++){
			double randomGen = randomSeeds.nextDouble();
			double result = start + (randomGen * (end - start));
			testStrings.add(result+"");
		}
		
		//testing workloads that are numberical values, in this case the try-catch approach will not never incur an exception
		System.out.println("Test with workloads that are all numerical values");
		workloadTest(testStrings, pa);
		System.out.println("----------------------------------------------------------------\n\n");

		
		for(int i=0; i<NUM_TEST_STRINGS/2; i++){
			testStrings.set(i, "A");
		}
		
		System.out.println("Test with workloads that half of them are numberical values");
		workloadTest(testStrings, pa);
		System.out.println("----------------------------------------------------------------\n\n");

		for(int i=NUM_TEST_STRINGS/2; i<NUM_TEST_STRINGS; i++){
			testStrings.set(i, "A");
		}
		System.out.println("Test with workloads that all them are numberical values");
		workloadTest(testStrings, pa);
		System.out.println("----------------------------------------------------------------\n\n");

	}

}
