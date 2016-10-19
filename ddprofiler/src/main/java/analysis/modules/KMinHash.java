/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

import java.util.List;
import java.util.Random;

import analysis.TextualDataConsumer;

public class KMinHash implements TextualDataConsumer {
	
	final private int K = 512;
	final private long MERSENNE_PRIME = (1 << 61) - 1;
	private long[] minhash;
	private long[][] rndSeeds;
	
	public KMinHash(int pseudoRandomSeed) {
		minhash = new long[K];
		rndSeeds = new long[K][2];
		Random rnd = new Random(pseudoRandomSeed);
		
		for(int i = 0; i < minhash.length; i++) {
			minhash[i] = Long.MAX_VALUE;
			long nextSeed = rnd.nextLong();
			rndSeeds[i][0] = nextSeed;
			nextSeed = rnd.nextLong();
			rndSeeds[i][1] = nextSeed;
		}
	}
	
	private static long hash(String string) {
		
		long h = (1 << 61) - 1; // prime
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31*h + string.charAt(i);
		}
		return h;
	}

	@Override
	public boolean feedTextData(List<String> records) {
		for (String r : records) {
			long rawHash = hash(r);
			for (int i = 0; i < K; i++) {
				// h = (a * x) + b
				long hash = (rndSeeds[i][0] * rawHash + rndSeeds[i][1]) % MERSENNE_PRIME;
				if(hash < minhash[i]) {
					minhash[i] = hash;
				}
			}
		}

		return true;
	}
	
	public long[] getMH() {
		return minhash;
	}
	
}