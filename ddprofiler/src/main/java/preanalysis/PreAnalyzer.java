/**
 * @author ra-mit
 * @author Sibo Wang (edit)
 */
package preanalysis;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.Main;
import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import inputoutput.conn.Connector;

public class PreAnalyzer implements PreAnalysis, IO {

	final private Logger LOG = LoggerFactory.getLogger(PreAnalyzer.class.getName());
	
	private Connector c;
	private List<Attribute> attributes;
	private boolean knownDataTypes = false;
	
	private static final Pattern _DOUBLE_PATTERN = Pattern
			.compile("[\\x00-\\x20]*[+-]?(NaN|Infinity|((((\\p{Digit}+)(\\.)?((\\p{Digit}+)?)"
					+ "([eE][+-]?(\\p{Digit}+))?)|(\\.((\\p{Digit}+))([eE][+-]?(\\p{Digit}+))?)|"
					+ "(((0[xX](\\p{XDigit}+)(\\.)?)|(0[xX](\\p{XDigit}+)?(\\.)(\\p{XDigit}+)))"
					+ "[pP][+-]?(\\p{Digit}+)))[fFdD]?))[\\x00-\\x20]*");
	
	private static final Pattern DOUBLE_PATTERN = Pattern.compile("^(\\+|-)?\\d+([\\,]\\d+)*([\\.]\\d+)?$");
	private static final Pattern INT_PATTERN = Pattern.compile("^(\\+|-)?\\d+$");
	
	private final static String[] BANNED = {"", "nan"};

	/**
	 * Implementation of IO interface
	 */

	@Override
	public Map<Attribute, Values> readRows(int num) {
		Map<Attribute, List<String>> data = null;
		try {
			data = c.readRows(num);
			if (data == null)
				return null;
		}
		catch (IOException | SQLException e) {
			e.printStackTrace();
		}

		// Calculate data types if not known yet
		if (!knownDataTypes) {
			calculateDataTypes(data);
		}

		Map<Attribute, Values> castData = new HashMap<>();
		// Cast map to the type
		for (Entry<Attribute, List<String>> e : data.entrySet()) {
			Values vs = null;
			AttributeType at = e.getKey().getColumnType();
			if (at.equals(AttributeType.FLOAT)) {
				List<Float> castValues = new ArrayList<>();
				vs = Values.makeFloatValues(castValues);
				for (String s : e.getValue()) {
					float f = 0f;
					if(DOUBLE_PATTERN.matcher(s).matches()) {
						s = s.replace(",", ""); // remove commas, floats should be indicated with dots
						f = Float.valueOf(s).floatValue();
					}
					else {
						continue;
					}
					castValues.add(f);
				}
			}
			else if (at.equals(AttributeType.INT)) {
				List<Long> castValues = new ArrayList<>();
				vs = Values.makeIntegerValues(castValues);
				for (String s : e.getValue()) {
					long f = 0;
					if(INT_PATTERN.matcher(s).matches()) {
						try {
							f = Long.valueOf(s).longValue();
						}
						catch(NumberFormatException nfe) {
							LOG.warn("Error while parsing: {}", nfe.getMessage());
							continue; // skip problematic value
						}
					}
					else {
						continue;
					}
					castValues.add(f);
				}
			}
			else if (at.equals(AttributeType.STRING)) {
				List<String> castValues = new ArrayList<>();
				vs = Values.makeStringValues(castValues);
				e.getValue().forEach(s -> castValues.add(s));
			}

			castData.put(e.getKey(), vs);
		}

		return castData;
	}

	private void calculateDataTypes(Map<Attribute, List<String>> data) {
		// estimate data type for each attribute
		for (Entry<Attribute, List<String>> e : data.entrySet()) {
			Attribute a = e.getKey();
			// Only if the type is not already known
			if ( !a.getColumnType().equals(AttributeType.UNKNOWN))
				continue;
			AttributeType aType = typeOfValue(e.getValue());
			if(aType == null) {
				continue; // Means that data was dirty/anomaly, so skip value
			}
			a.setColumnType(aType);
		}
	}

	public static boolean isNumerical(String s) {
		return DOUBLE_PATTERN.matcher(s).matches();
	}

	private static boolean isInteger(String s) {
		if (s == null) {
			return false;
		}

		int length = s.length();
		if (length == 0) {
			return false;
		}
		int i = 0;
		if (s.charAt(0) == '-' || s.charAt(0) == '+') {
			if (length == 1) {
				return false;
			}
			i = 1;
		}
		for (; i < length; i++) {
			char c = s.charAt(i);
			if (c < '0' || c > '9') {
				return false;
			}
		}
		return true;
	}

	/*
	 * exception based type checker private static boolean
	 */
	public static boolean isNumericalException(String s) {
		try {
			Double.parseDouble(s);
			return true;
		} catch (NumberFormatException nfe) {
			return false;
		}
	}

	/**
	 * Figure out data type
	 * @param values
	 * @return
	 */

	public static AttributeType typeOfValue(List<String> values) {
		boolean isFloat = false;
		boolean isInt = false;
		int floatMatches = 0;
		int intMatches = 0;
		int strMatches = 0;
		for (String s : values) {
			s = s.trim();
			if (isBanned(s)) {
				// TODO: we'll piggyback at this point to figure out how to report cleanliness profile
				continue;
			}
			if (isNumerical(s)) {
				if (isInteger(s)) {
					intMatches++;
				}
				else {
					floatMatches++;
				}
			} 
			else {
				strMatches++;
			}
		}

		if (strMatches == 0) {
			if (floatMatches > 0)
				isFloat = true;
			else if (intMatches > 0)
				isInt = true;
		}

		if (isFloat)
			return AttributeType.FLOAT;
		if (isInt)
			return AttributeType.INT;
		return AttributeType.STRING;
	}
	
	private static boolean isBanned(String s) {
		String toCompare = s.trim().toLowerCase();
		for (String ban : BANNED) {
			if(toCompare.equals(ban)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Implementation of PreAnalysis interface
	 */

	@Override
	public void composeConnector(Connector c) {
		this.c = c;
		try {
			this.attributes = c.getAttributes();
		} catch (IOException | SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public DataQualityReport getQualityReport() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Attribute> getEstimatedDataTypes() {
		return attributes;
	}

}
