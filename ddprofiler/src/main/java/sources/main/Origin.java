package sources.main;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import core.Conductor;
import sources.SourceType;
import sources.config.SourceConfig;
import sources.connectors.Attribute;
import sources.connectors.Connector;

public interface Origin {

    /**
     * Given a SourceConfig that configures access to a source, create necessary
     * tasks and submit them to the Conductor for processing
     * 
     * @param config
     * @param c
     */
    public List<Origin> processSource(SourceConfig config, Conductor c);

    /**
     * Return the source type
     * 
     * @return
     */
    public SourceType getSourceType();

    /**
     * Initialize connector to read data if necessary
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public void initConnector() throws IOException, ClassNotFoundException, SQLException;

    /**
     * Free any resources associated to this connector
     */
    public void destroyConnector();

    /**
     * Obtain the attributes for of the given source
     * 
     * @return
     * @throws IOException
     * @throws SQLException
     */
    public List<Attribute> getAttributes() throws IOException, SQLException;

    /**
     * Reads the actual source and returns the values along with each
     * attribute's info
     * 
     * @param num
     * @return
     * @throws IOException
     * @throws SQLException
     */
    public Map<Attribute, List<String>> readRows(int num) throws IOException, SQLException;

    /**
     * Returns the original source config used to configure the current source
     * 
     * @return
     */
    public SourceConfig getSourceConfig();

    /**
     * If this works as a task, returns the task ID FIXME: this does not belong
     * here, need to find new iface
     * 
     * @return
     */
    public int getTaskId();

    /**
     * Returns the connector with the above interface FIXME: should dissappear
     * as this iface is implemented by this
     * 
     * @return
     */
    public Connector getConnector();

    /**
     * FIXME: overlaps with destroy connector
     */
    public void close();

}
