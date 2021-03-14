package de.webis.cikm20_duplicates.util;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.aitools.ir.fingerprinting.hashfunction.SimHash;
import de.aitools.ir.fingerprinting.representation.HashVector;
import de.aitools.ir.fingerprinting.representation.HashVectorSha3;
import de.aitools.ir.fingerprinting.representer.Hash;
import de.webis.cikm20_duplicates.spark.eval.SparkEvaluateSimHashFeatures;
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
		public List<T> fingerprint(List<String> features);
		
		public double similarity(List<T> a, List<T> b);
		public String fingerprinterName();
		
		public default double similarity(CollectionDocument a, CollectionDocument b) {
			List<T> fingerprintA = fingerprint(a);
			List<T> fingerprintB = fingerprint(b);
			
			return similarity(fingerprintA, fingerprintB);
		}
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
			public List<Integer> fingerprint(List<String> features) {
				throw new RuntimeException();
			}

			@Override
			public double similarity(List<Integer> a, List<Integer> b) {
				int[] aArray = a.stream().mapToInt(i -> i).toArray();
				int[] bArray = b.stream().mapToInt(i -> i).toArray();
				
				return minHash.similarity(aArray, bArray);
			}
			
			public String fingerprinterName() {
				return "MinHashWithJavaHash";
			}
		};
	}
	
	@SuppressWarnings("serial")
	public static Fingerprinter<Integer> simHashFingerPrinting(int bitsInSimHash, int k) {
		return new Fingerprinter<Integer>() {

			@Override
			public List<Integer> fingerprint(CollectionDocument doc) {
				byte[] hash = hashVector(doc, bitsInSimHash);
				
				return HashTransformationUtil.hashToIntegers(hash, k);
			}

			@Override
			public List<Integer> fingerprint(List<String> features) {
				byte[] hash = hashVector(features, bitsInSimHash);
				
				return HashTransformationUtil.hashToIntegers(hash, k);
			}
			
			@Override
			public double similarity(List<Integer> a, List<Integer> b) {
				byte[] aArr = HashTransformationUtil.integersToHash(a);
				byte[] bArr = HashTransformationUtil.integersToHash(b);
				
				return 1.0 - (((double)Hash.getHammingDistance(aArr, bArr))/((double)aArr.length*8));
			}
			
			public String fingerprinterName() {
				return "64BitK3SimHashOneGramms";
			}
		};
	}
	
	@SuppressWarnings("serial")
	public static Fingerprinter<Integer> productionFingerpringint(int bitsInSimHash, int k) {
		return new Fingerprinter<Integer>() {

			@Override
			public List<Integer> fingerprint(CollectionDocument doc) {
				byte[] hash = hashVector(SparkEvaluateSimHashFeatures.theeAndFiveGramms(doc), bitsInSimHash);
				
				return HashTransformationUtil.hashToIntegers(hash, k);
			}

			@Override
			public List<Integer> fingerprint(List<String> features) {
				byte[] hash = hashVector(features, bitsInSimHash);
				
				return HashTransformationUtil.hashToIntegers(hash, k);
			}
			
			@Override
			public double similarity(List<Integer> a, List<Integer> b) {
				byte[] aArr = HashTransformationUtil.integersToHash(a);
				byte[] bArr = HashTransformationUtil.integersToHash(b);
				
				return 1.0 - (((double)Hash.getHammingDistance(aArr, bArr))/((double)aArr.length*8));
			}
			
			public String fingerprinterName() {
				return "64BitK3SimHashThreeAndFiveGramms";
			}
		};
	}
	
	static byte[] hashVector(CollectionDocument doc, int bitsInSimHash) {
		return new SimHash().hash(internalHashVector(doc, bitsInSimHash)).getArray();
	}
	
	static byte[] hashVector(List<String> terms, int bitsInSimHash) {
		return new SimHash().hash(internalHashVector(terms, bitsInSimHash)).getArray();
	}
	
	public static HashVector internalHashVector(CollectionDocument doc, int bitsInSimHash) {
		List<String> terms = NGramms.tokenize(doc.getFullyCanonicalizedContent());
		return internalHashVector(terms, bitsInSimHash);
	}
	
	public static HashVector internalHashVector(List<String> terms, int bitsInSimHash) {
		return HashVectorSha3.toVector(terms, bitsInSimHash);
	}
	
	private static Set<Integer> docToElementSet(CollectionDocument doc) {
		return NGramms.build8Gramms(doc.getFullyCanonicalizedContent()).stream()
				.map(i -> i.getMd5Hash().hashCode())
				.collect(Collectors.toSet());
	}
}
