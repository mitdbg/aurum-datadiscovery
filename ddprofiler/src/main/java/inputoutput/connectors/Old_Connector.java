/**
 * @author Sibo Wang
 * @author ra-mit (edits)
 *
 */

package inputoutput.connectors;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;

import inputoutput.Attribute;
import inputoutput.Record;
import metrics.Metrics;

public abstract class Old_Connector {

    final private Logger LOG = LoggerFactory.getLogger(Old_Connector.class.getName());

    // Metrics on how many successful and erroneous records are processed
    private Counter error_records = Metrics.REG.counter((name(Old_Connector.class, "error", "records")));
    private Counter success_records = Metrics.REG.counter((name(Old_Connector.class, "success", "records")));

    public abstract String getDBName();

    public abstract String getPath();

    public abstract String getSourceName();

    abstract void initConnector() throws IOException, ClassNotFoundException, SQLException;

    abstract void destroyConnector();

    public abstract List<Attribute> getAttributes() throws IOException, SQLException;

    public abstract boolean readRows(int num, List<Record> rec_list) throws IOException, SQLException;

    public abstract void close();

    /**
     * Returns a map with Attribute of table as key and a list of num values as
     * value. Map is created internally as it must preserve insertion order
     * 
     * @param num
     * @return
     * @throws IOException
     * @throws SQLException
     */
    public Map<Attribute, List<String>> readRows(int num) throws IOException, SQLException {

	Map<Attribute, List<String>> data = new LinkedHashMap<>();
	// Make sure attrs is populated, if not, populate it here
	if (data.isEmpty()) {
	    List<Attribute> attrs = this.getAttributes();
	    attrs.forEach(a -> data.put(a, new ArrayList<>()));
	}

	// Read data and insert in order
	List<Record> recs = new ArrayList<>();
	boolean readData = this.readRows(num, recs);
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

}
