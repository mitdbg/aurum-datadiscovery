package test;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class E2ECompareTest {

    String filePath = "/Users/gina/Documents/UROP/aurum/ddprofiler/src/test/python/test";
    String pathToExpected = "master.dd";
    String pathToResults = "load-balance.dd";

    private void parseDDToMap(String path, Map map) {

        BufferedReader br = null;
        String line = "";

        try {
            br = new BufferedReader(new FileReader(path));
            while ((line = br.readLine()) != null) {
                Node node = new Node(line);
                map.put(node.id, node);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void testNumberOfColumns() {
        Map<String, Node> expected = new HashMap<>();
        Map<String, Node> results = new HashMap<>();

        parseDDToMap(filePath + pathToExpected, expected);
        parseDDToMap(filePath + pathToResults, results);

        assertEquals("Expected " + expected.size() + " columns but got " + results.size(),
                expected.size(), results.size());
    }

    @Test
    public void testTextualStatistics() {
        Map<String, Node> expected = new HashMap<>();
        Map<String, Node> results = new HashMap<>();
        List<CompareError> errorList = new ArrayList<>();

        parseDDToMap(filePath + pathToExpected, expected);
        parseDDToMap(filePath + pathToResults, results);

        int successes = 0;
        int errors = 0;
        for (Node result : results.values()) {
            if (result.dataType.equals("T")) {
                Node compareTo = expected.get(result.id);
                if (compareTo.equals(result)) {
                    successes++;
                } else {
                    errorList.add(new CompareError(compareTo, result));
                    errors++;
                }
            }
        }
        System.out.println("testTextualStatistics: " + successes + " successes and " + errors + " errors.");
        assertEquals(errorList, Collections.emptyList());
    }

    @Test
    public void testNumericalStatistics() {
        Map<String, Node> expected = new HashMap<>();
        Map<String, Node> results = new HashMap<>();
        List<CompareError> errorList = new ArrayList<>();

        parseDDToMap(filePath + pathToExpected, expected);
        parseDDToMap(filePath + pathToResults, results);

        int successes = 0;
        int errors = 0;
        for (Node result : results.values()) {
            if (result.dataType.equals("N")) {
                Node compareTo = expected.get(result.id);
                if (compareTo.equals(result)) {
                    successes++;
                } else {
                    errorList.add(new CompareError(compareTo, result));
                    errors++;
                }
            }
        }
        System.out.println("testNumericalStatistics: " + successes + " successes and " + errors + " errors.");
        assertEquals(errorList, Collections.emptyList());
    }
}

class Node {

    final String id;
    final String dbName;
    final String sourceName;
    final String columnName;
    final int totalValues;
    final int uniqueValues;
    final String dataType;
    final float minValue;
    final float maxValue;
    final float avgValue;
    final float median;
    final float iqr;

    public Node(String line) {
        String[] values = line.split(",");

        id = values[0];
        dbName = values[1];
        sourceName = values[2];
        columnName = values[3];
        totalValues = Integer.parseInt(values[4]);
        uniqueValues = Integer.parseInt(values[5]);
        dataType = values[6];

        if (dataType.equals("N")) {
            minValue = Float.parseFloat(values[7]);
            maxValue = Float.parseFloat(values[8]);
            avgValue = Float.parseFloat(values[9]);
            median = Float.parseFloat(values[10]);
            iqr = Float.parseFloat(values[11]);
        } else {
            minValue = 0;
            maxValue = 0;
            avgValue = 0;
            median = 0;
            iqr = 0;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Node)) {
            return false;
        }
        Node node = (Node) obj;
        return node.dbName.equals(dbName) && node.sourceName.equals(sourceName) && node.columnName.equals(columnName) &&
                node.totalValues == totalValues && node.uniqueValues == uniqueValues && node.dataType.equals(dataType) &&
                node.minValue == minValue && node.maxValue == maxValue && node.avgValue == node.avgValue &&
                node.median == median && node.iqr == iqr;
    }
}

class CompareError {

    final Node expected;
    final Node result;

    public CompareError(Node expected, Node result) {
        this.expected = expected;
        this.result = result;
    }
}