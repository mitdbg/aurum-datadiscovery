/**
 * @author Raul - raulcf@csail.mit.edu
 * @author Sibo Wang (edits)
 */
package analysis.modules;

import java.util.List;
import com.clearspring.analytics.stream.quantile.QDigest;
import analysis.FloatDataConsumer;
import analysis.IntegerDataConsumer;

public class RangeAnalyzer implements IntegerDataConsumer, FloatDataConsumer {

	private int totalRecords;
	private int max;
	private int min;
	private int totalSum;
	
	private float maxF;
	private float minF;
	private float totalSumF;
	
	
	/*
	 * calculate the std_deviation
	 */
	private double square_Sum;
	private float avg;
	private float std_deviation;

	private final int QUANTILE_COMPRESSION_RATIO=128;
	/*
	 * provide estimator of quantile.
	 * Let c be the number of distinct values in the stream
	 * the relative error is O(log(c)/QUANTILE_COMPRESSION_RATIO)
	 * We make a conservative assumption that c can be as large as 2^64. 
	 * Then, to provide good estimation, we will need to set QUANTILE_COMPRESSION_RATIO
	 * to 128 to reach a reasonable relative estimation.
	 */
	private QDigest quantile_estimator = new QDigest(QUANTILE_COMPRESSION_RATIO);

	public long getQuantile(double p){
		return quantile_estimator.getQuantile(p);
	}
	
	public Range getIntegerRange() {
		avg = (float) (totalSum*1.0/totalRecords);
		std_deviation = (float) Math.sqrt(square_Sum/totalRecords - avg*avg);
		Range r = new Range(DataType.Type.INT, totalRecords, max, min, avg, std_deviation);
		return r;
	}
	
	public Range getFloatRange() {
		avg = totalSumF/totalRecords;
		std_deviation = (float) Math.sqrt(square_Sum/totalRecords - avg*avg);
		Range r = new Range(DataType.Type.FLOAT, totalRecords, maxF, minF, avg, std_deviation);
		return r;
	}

	@Override
	public boolean feedIntegerData(List<Integer> records) {
		
		for(int value : records) {
			totalRecords++;
			if(value > max) max = value;
			if(value < min) min = value;
			totalSum += value;
			square_Sum +=value*value;
			quantile_estimator.offer(value);
		}
		
		return true;
	}
	
	@Override
	public boolean feedFloatData(List<Float> records) {
		
		for(float value : records) {
			totalRecords++;
			if(value > maxF) maxF = value;
			if(value < minF) minF = value;
			totalSumF += value;
			square_Sum += value*value;
			quantile_estimator.offer((long) value);
		}
		return true;
	}

}
