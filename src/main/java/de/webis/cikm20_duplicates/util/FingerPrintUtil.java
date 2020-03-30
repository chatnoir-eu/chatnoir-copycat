package de.webis.cikm20_duplicates.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.aitools.ir.fingerprinting.hashfunction.SimHash;
import de.aitools.ir.fingerprinting.representation.HashVector;
import de.aitools.ir.fingerprinting.representation.HashVectorSha3;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.util.NGramms;
import info.debatty.java.lsh.MinHash;
import lombok.experimental.UtilityClass;

/**
 * 
 * @author Maik Fröbe
 *
 */
@UtilityClass
public class FingerPrintUtil {
	public static interface Fingerprinter<T extends Comparable<T>> extends Serializable {
		public List<T> fingerprint(CollectionDocument doc);
		
		public double similarity(List<T> a, List<T> b);
	}

	@SuppressWarnings("serial")
	public static Fingerprinter<Integer> minHashFingerPrinting(long seed) {
		int dict_size = 1;
		int size = 12;
		MinHash minHash = new MinHash(size, dict_size, seed);
		
		return new Fingerprinter<Integer>() {
			@Override
			public List<Integer> fingerprint(CollectionDocument doc) {
				return IntStream.of(minHash.signature(docToElementSet(doc))).boxed().collect(Collectors.toList());
			}

			@Override
			public double similarity(List<Integer> a, List<Integer> b) {
				int[] aArray = a.stream().mapToInt(i -> i).toArray();
				int[] bArray = b.stream().mapToInt(i -> i).toArray();
				
				return minHash.similarity(aArray, bArray);
			}
		};
	}
	
	@SuppressWarnings("serial")
	public static Fingerprinter<Integer> simHashFingerPrinting(int bitsInSimHash, int k) {
		return new Fingerprinter<Integer>() {

			@Override
			public List<Integer> fingerprint(CollectionDocument doc) {
				byte[] hash = hashVector(doc, bitsInSimHash);
				
				return hashToIntegers(hash, k);
			}

			@Override
			public double similarity(List<Integer> a, List<Integer> b) {
				return 1.0;
			}
		};
	}
	
	static List<Integer> hashToIntegers(byte[] hash, int k) {
		if(k != 3 || hash == null || hash.length != 8) {
			throw new IllegalArgumentException("");
		}
		
		return Arrays.asList(
			// 0-15. Use 0 bytes at positions 3 and 4 to identify bytes 0-15.
			HashTransformationUtil.bytesToInt(new byte[] {hash[0], hash[1], 0x0, 0x0}),
			// 16-31. Use 0 bytes at positions 1 and 2 to identify bytes 16-31.
			HashTransformationUtil.bytesToInt(new byte[] {0x0, 0x0, hash[2], hash[3]}),
			// 32-47. Use 0 bytes at positions 2 and 4 to identify bytes 32-47.
			HashTransformationUtil.bytesToInt(new byte[] {0x0, hash[4], 0x0, hash[5]}),
			// 48-63. Use 0 bytes at positions 1 and 4 to identify bytes 48-63.
			HashTransformationUtil.bytesToInt(new byte[] {0x0, hash[6], hash[7], 0x0})
		);
	}
	
	static byte[] hashVector(CollectionDocument doc, int bitsInSimHash) {
		List<String> terms = NGramms.tokenize(doc.getFullyCanonicalizedContent());
		HashVector vector = HashVectorSha3.toVector(terms, bitsInSimHash);
		
		return new SimHash().hash(vector).getArray();
	}
	
	private static Set<Integer> docToElementSet(CollectionDocument doc) {
		return NGramms.build8Gramms(doc.getFullyCanonicalizedContent()).stream()
				.map(i -> i.getMd5Hash().hashCode())
				.collect(Collectors.toSet());
	}
}
