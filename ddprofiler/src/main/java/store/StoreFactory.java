package store;

import core.config.ProfilerConfig;

public class StoreFactory {

	public static Store makeElasticStore(ProfilerConfig pc) {
		return new ElasticStore(pc);
	}
	
	public static Store makeNullStore(ProfilerConfig pc) {
		return new NullStore();
	}
	
}
