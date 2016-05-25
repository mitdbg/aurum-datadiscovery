package store;

import core.config.ProfilerConfig;

public class StoreFactory {

	public static Store makeElasticStore(ProfilerConfig pc) {
		return new ElasticStore(pc);
	}
}
