package de.webis.cikm20_duplicates.util;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class SimHash64BitK3Test {
	
	private static final CollectionDocument
	DOC1 = doc(1, "a b c a"),
	DOC2 = doc(2, "a a c d"),
	DOC3 = doc(3, "a b c d"),
	DOC4 = doc(4, "k b c d"),
	DOC5 = doc(5, "e e e e"),
	DOC6 = doc(6, "e e e e"),
	DOC7 = doc(7, "e f e e"),
	DOC8 = doc(8, "e e g e");
	
	private static List<CollectionDocument> DOCS = Arrays.asList(
			DOC1, DOC2, DOC3, DOC4, DOC5, DOC6, DOC7, DOC8
	);
	
	private Fingerprinter<Integer> fingerprinter;
	
	private final double delta = 1e-3;
	
	@Before
	public void setUp() {
		int k = 3;
		int bitsInSimHash = 64;
		fingerprinter = FingerPrintUtil.simHashFingerPrinting(bitsInSimHash, k);
	}
	
	@Test
	public void testFingerprintSimilarityIsReflexive() {
		for(CollectionDocument doc: DOCS) {
			Assert.assertTrue(candidates(doc, doc));
		}
	}
	
	@Test
	public void testFingerprintSimilarityIsReturns1ForTheSameElement() {
		double expected = 1.0;
		
		for(CollectionDocument doc: DOCS) {
			double actual = sim(doc, doc);
			Assert.assertEquals(expected, actual, delta);
		}
	}
	
	@Test
	public void approveSimilarityForDoc1ComparedToDoc2() {
		Assert.assertTrue(candidates(DOC1, DOC2));
		Assert.assertEquals(0.7, sim(DOC1, DOC2), delta);
	}
	
	private boolean candidates(CollectionDocument a, CollectionDocument b) {
		List<Integer> hashOfA = fingerprinter.fingerprint(a);
		List<Integer> hashOfB = fingerprinter.fingerprint(b);
		
		return hashOfA != null && hashOfB != null && hashOfA.stream().anyMatch(i -> hashOfB.contains(i));
	}

	private double sim(CollectionDocument a, CollectionDocument b) {
		return fingerprinter.similarity(fingerprinter.fingerprint(a), fingerprinter.fingerprint(b));
	}
	
	private static CollectionDocument doc(int id, String fullyCanonicalizedContent) {
		return new CollectionDocument("doc" + id, null, fullyCanonicalizedContent);
	};
}
