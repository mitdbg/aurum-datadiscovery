package sources;

import core.Conductor;
import core.config.sources.SourceConfig;

public interface Source {

    public void processSource(SourceConfig config, Conductor c);

}
