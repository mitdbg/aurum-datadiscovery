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

import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import inputoutput.conn.Connector;

public class PreAnalyzer implements PreAnalysis, IO {

	private Connector c;
	private List<Attribute> attributes;
	private boolean knownDataTypes = false;
	
	private static final Pattern DOUBLE_PATTERN = Pattern
			.compile("[\\x00-\\x20]*[+-]?(NaN|Infinity|((((\\p{Digit}+)(\\.)?((\\p{Digit}+)?)"
					+ "([eE][+-]?(\\p{Digit}+))?)|(\\.((\\p{Digit}+))([eE][+-]?(\\p{Digit}+))?)|"
					+ "(((0[xX](\\p{XDigit}+)(\\.)?)|(0[xX](\\p{XDigit}+)?(\\.)(\\p{XDigit}+)))"
					+ "[pP][+-]?(\\p{Digit}+)))[fFdD]?))[\\x00-\\x20]*");

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
			//knownDataTypes = true;
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
					try {
						f = Float.valueOf(s).floatValue();
					} catch (NumberFormatException nfe) {
						continue; // SKIP data that does not parse correctly
					}
					castValues.add(f);
				}
			}
			else if (at.equals(AttributeType.INT)) {
				List<Integer> castValues = new ArrayList<>();
				vs = Values.makeIntegerValues(castValues);
				for (String s : e.getValue()) {
					int f = 0;
					try {
						f = Integer.valueOf(s).intValue();
					} catch (NumberFormatException nfe) {
						continue; // SKIP data that does not parse correctly
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
			if (!a.getColumnType().equals(AttributeType.UNKNOWN))
				continue;
			AttributeType aType = typeOfValue(e.getValue());
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
	 * FIXME: Will always choose FLOAT or STRING. fix it to choose INT when
	 * appropriate
	 * 
	 * Fixed the type checking issue
	 * 
	 * @param values
	 * @return
	 */

	private AttributeType typeOfValue(List<String> values) {
		boolean isFloat = false;
		boolean isInt = false;
		int floatMatches = 0;
		int intMatches = 0;
		int strMatches = 0;
		for (String s : values) {
			if (isNumerical(s)) {
				if (isInteger(s))
					intMatches++;
				else
					floatMatches++;
			} else {
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
