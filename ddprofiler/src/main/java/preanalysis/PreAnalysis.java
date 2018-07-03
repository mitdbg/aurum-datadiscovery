/**
 * @author ra-mit
 */
package preanalysis;

import java.util.List;

import sources.connectors.Attribute;
import sources.connectors.Connector;

public interface PreAnalysis {

    public void composeConnector(Connector c);

    public DataQualityReport getQualityReport();

    public List<Attribute> getEstimatedDataTypes();
}
