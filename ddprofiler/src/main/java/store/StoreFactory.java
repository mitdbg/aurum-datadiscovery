package store;

import core.config.ProfilerConfig;

public class StoreFactory {

	public static Store makeHttpElasticStore(ProfilerConfig pc) {
		return new HttpElasticStore(pc);
	}
	
	public static Store makeNativeElasticStore(ProfilerConfig pc) {
		return new NativeElasticStore(pc);
	}
	
	public static Store makeNullStore(ProfilerConfig pc) {
		return new NullStore();
	}
	
}
