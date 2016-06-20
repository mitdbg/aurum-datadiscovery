package test;

import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.junit.Test;

public class PatternTest {

	@Test
	public void testPatterns() {
		String input0 = "0";
		String input1 = "9";
		String input2 = "23";
		String input3 = "9772";
		String input4 = "-324";
		String input5 = "54";
		String input6 = "-23432456";
		String input7 = "13457143587";
		
		String regex = "^(\\+|-)?\\d+$";
		Pattern intPattern = Pattern.compile("^(\\+|-)?\\d+$");
		
		boolean r0 = intPattern.matcher(input0).matches();
		boolean r1 = intPattern.matcher(input1).matches();
		boolean r2 = intPattern.matcher(input2).matches();
		boolean r3 = intPattern.matcher(input3).matches();
		boolean r4 = intPattern.matcher(input4).matches();
		boolean r5 = intPattern.matcher(input5).matches();
		boolean r6 = intPattern.matcher(input6).matches();
		boolean r7 = intPattern.matcher(input7).matches();
		
		assertTrue("0 - OK", r0);
		assertTrue("1 - OK", r1);
		assertTrue("2 - OK", r2);
		assertTrue("3 - OK", r3);
		assertTrue("4 - OK", r4);
		assertTrue("5 - OK", r5);
		assertTrue("6 - OK", r6);
		assertTrue("7 - OK", r7);
	}
	
}
