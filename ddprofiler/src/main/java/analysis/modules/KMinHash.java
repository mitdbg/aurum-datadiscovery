/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

import java.util.List;
import java.util.Random;

import analysis.TextualDataConsumer;
import core.MurmurHash3;

public class KMinHash implements TextualDataConsumer {
	
	final private int K = 256;
	final private int SEED = 666; // Not caring about security for now
	private int[] minhash;
	private int[] rndSeeds;
	
	public KMinHash() {
		minhash = new int[K];
		rndSeeds = new int[K];
		Random rnd = new Random();
		for(int i = 0; i < minhash.length; i++) {
			minhash[i] = Integer.MAX_VALUE;
			int nextSeed = rnd.nextInt();
			rndSeeds[i] = nextSeed;
		}
	}

	@Override
	public boolean feedTextData(List<String> records) {

		for (String r : records) {
			int rawHash = MurmurHash3.murmurhash3_x86_32(r, 0, r.length(), SEED);
			for (int i = 0; i < K; i++) {
				int hash = (rawHash >>> (i+1)) ^ rndSeeds[i]; // rotation + XOR
				if(hash < minhash[i]) {
					minhash[i] = hash;
				}
			}
		}

		return true;
	}
	
	public int[] getMH() {
		return minhash;
	}
	
}