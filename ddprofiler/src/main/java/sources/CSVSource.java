package sources;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.Conductor;
import sources.config.CSVSourceConfig;
import sources.config.SourceConfig;
import sources.tasks.ProfileTask;
import sources.tasks.ProfileTaskFactory;

public class CSVSource implements Source {

    final private Logger LOG = LoggerFactory.getLogger(CSVSource.class.getName());

    @Override
    public void processSource(SourceConfig config, Conductor c) {
	assert (config instanceof CSVSourceConfig);

	CSVSourceConfig csvConfig = (CSVSourceConfig) config;
	String pathToSources = csvConfig.getPath();

	// TODO: at this point we'll be harnessing metadata from the source

	File folder = new File(pathToSources);
	int totalFiles = 0;
	int tt = 0;

	File[] filePaths = folder.listFiles();
	for (File f : filePaths) {
	    tt++;
	    if (f.isFile()) {
		String path = f.getParent() + File.separator;
		String name = f.getName();

		// Make the csv config specific to the relation
		CSVSourceConfig relationCSVConfig = (CSVSourceConfig) csvConfig.selfCopy();
		relationCSVConfig.setRelationName(name);
		relationCSVConfig.setPath(path);

		ProfileTask pt = ProfileTaskFactory.makeCSVProfileTask(relationCSVConfig);

		// TaskPackage tp =
		// TaskPackage.makeCSVFileTaskPackage(csvConfig.getSourceName(),
		// path, name,
		// csvConfig.getSeparator());

		totalFiles++;
		c.submitTask(pt);
	    }
	}

	LOG.info("Total files submitted for processing: {} - {}", totalFiles, tt);
    }

}
