/**
 * @author ra-mit
 */
package preanalysis;

import java.util.List;

import sources.deprecated.Attribute;
import sources.deprecated.Connector;

public interface PreAnalysis {

    public void composeConnector(Connector c);

    public DataQualityReport getQualityReport();

    public List<Attribute> getEstimatedDataTypes();
}
