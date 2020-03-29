package de.webis.cikm20_duplicates.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
				return Arrays.asList(1,1,1,1);
			}

			@Override
			public double similarity(List<Integer> a, List<Integer> b) {
				return 1.0;
			}
		};
	}
	
	private static Set<Integer> docToElementSet(CollectionDocument doc) {
		return NGramms.build8Gramms(doc.getFullyCanonicalizedContent()).stream()
				.map(i -> i.getMd5Hash().hashCode())
				.collect(Collectors.toSet());
	}
}
