package inputoutput.connectors;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;

import au.com.bytecode.opencsv.CSVReader;
import core.SourceType;
import core.config.sources.CSVSourceConfig;
import inputoutput.Attribute;
import inputoutput.Record;
import inputoutput.TableInfo;
import metrics.Metrics;

public class CSVConnector implements Connector {

    final private Logger LOG = LoggerFactory.getLogger(CSVConnector.class.getName());

    private CSVSourceConfig config;
    private CSVReader fileReader;

    // Connector state
    private long lineCounter = 0;
    private TableInfo tableInfo;

    // Metrics
    // Metrics on how many successful and erroneous records are processed
    private Counter error_records = Metrics.REG.counter((name(CSVConnector.class, "error", "records")));
    private Counter success_records = Metrics.REG.counter((name(CSVConnector.class, "success", "records")));

    public CSVConnector(CSVSourceConfig config) {
	this.config = config;

	this.tableInfo = new TableInfo();
	try {
	    initConnector();
	} catch (ClassNotFoundException | IOException | SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	List<Attribute> attrs = null;
	try {
	    attrs = this.getAttributes();
	} catch (IOException | SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	tableInfo.setTableAttributes(attrs);
    }

    @Override
    public SourceType getSourceType() {
	return SourceType.csv;
    }

    @Override
    public void initConnector() throws IOException, ClassNotFoundException, SQLException {
	String path = config.getPath() + config.getRelationName();
	char separator = config.getSeparator().charAt(0);
	fileReader = new CSVReader(new FileReader(path), separator);
    }

    @Override
    public List<Attribute> getAttributes() throws IOException, SQLException {
	// assume that the first row is the attributes;
	if (lineCounter != 0) {
	    return tableInfo.getTableAttributes();
	}
	String[] attributes = fileReader.readNext();
	lineCounter++;

	List<Attribute> attrList = new ArrayList<Attribute>();
	for (int i = 0; i < attributes.length; i++) {
	    Attribute attr = new Attribute(attributes[i]);
	    attrList.add(attr);
	}
	return attrList;
    }

    @Override
    public Map<Attribute, List<String>> readRows(int num, List<Record> rec_list) throws IOException, SQLException {
	Map<Attribute, List<String>> data = new LinkedHashMap<>();
	// Make sure attrs is populated, if not, populate it here
	if (data.isEmpty()) {
	    List<Attribute> attrs = this.getAttributes();
	    attrs.forEach(a -> data.put(a, new ArrayList<>()));
	}

	// Read data and insert in order
	List<Record> recs = new ArrayList<>();
	boolean readData = this.read(num, recs);
	if (!readData) {
	    return null;
	}

	for (Record r : recs) {
	    List<String> values = r.getTuples();
	    int currentIdx = 0;
	    if (values.size() != data.values().size()) {
		error_records.inc();
		continue; // Some error while parsing data, a row has a
			  // different format
	    }
	    success_records.inc();
	    for (List<String> vals : data.values()) { // ordered iteration
		vals.add(values.get(currentIdx));
		currentIdx++;
	    }
	}
	return data;
    }

    private boolean read(int numRecords, List<Record> rec_list) throws IOException {
	boolean read_lines = false;
	String[] res = null;
	for (int i = 0; i < numRecords && (res = fileReader.readNext()) != null; i++) {
	    lineCounter++;
	    read_lines = true;
	    Record rec = new Record();
	    rec.setTuples(res);
	    rec_list.add(rec);
	}
	return read_lines;
    }

    @Override
    public void destroyConnector() {
	try {
	    fileReader.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

}
