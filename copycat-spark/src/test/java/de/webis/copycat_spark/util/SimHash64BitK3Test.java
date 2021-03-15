package de.webis.copycat_spark.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.aitools.ir.fingerprinting.representer.Hash;
import de.webis.copycat_spark.util.FingerPrintUtil;
import de.webis.copycat_spark.util.HashTransformationUtil;
import de.webis.copycat_spark.util.FingerPrintUtil.Fingerprinter;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class SimHash64BitK3Test {
	
	private static final CollectionDocument
	DOC1 = doc(1, "a b c a"),
	DOC2 = doc(2, "a d a c d c"),
	DOC3 = doc(3, "a c c d"),
	DOC4 = doc(4, "e e f f g g h h i i j j k k l l m m n n o o p p q q r r"),
	DOC5 = doc(5, "e e f f g g h h i i j j k k l l m m n n o o p p q q r r l m"),
	DOC6 = doc(6, "e e e e f f f a a a g g"),
	DOC7 = doc(7, "e e e e f f f a a a g"),
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
	public void testFingerprintSimilarityReturns1ForTheSameElement() {
		double expected = 1.0;
		
		for(CollectionDocument doc: DOCS) {
			double actual = sim(doc, doc);
			Assert.assertEquals(expected, actual, delta);
			Assert.assertEquals(0, dist(doc, doc));
		}
	}
	
	@Test
	public void testHammingDistanceIsZeroForTheSameElement() {

		for(CollectionDocument doc: DOCS) {
			Assert.assertEquals(0, dist(doc, doc));
		}
	}
	
	@Test
	public void approveSimilarityForDoc2ComparedToDoc3() {
		Assert.assertTrue(candidates(DOC2, DOC3));
		Assert.assertEquals(0.9687, sim(DOC2, DOC3), delta);
		Assert.assertEquals(2, dist(DOC2, DOC3));
	}
	
	@Test
	public void approveSimilarityForDoc1ComparedToDoc3() {
		Assert.assertFalse(candidates(DOC1, DOC3));
		Assert.assertEquals(0.75, sim(DOC1, DOC3), delta);
		Assert.assertEquals(16, dist(DOC1, DOC3));
	}
	
	@Test
	public void approveSimilarityForDoc4ComparedToDoc5() {
		Assert.assertTrue(candidates(DOC4, DOC5));
		Assert.assertEquals(0.96875, sim(DOC4, DOC5), delta);
		Assert.assertEquals(2, dist(DOC4, DOC5));
	}
	
	@Test
	public void approveSimilarityForDoc3ComparedToDoc4() {
		Assert.assertFalse(candidates(DOC3, DOC4));
		Assert.assertEquals(0.5, sim(DOC3, DOC4), delta);
		Assert.assertEquals(32, dist(DOC3, DOC4));
	}
	
	@Test
	public void approveSimilarityForDoc6ComparedToDoc7() {
		Assert.assertTrue(candidates(DOC6, DOC7));
		Assert.assertEquals(0.9843, sim(DOC6, DOC7), delta);
		Assert.assertEquals(1, dist(DOC6, DOC7));
	}
	
	@Test
	public void approveSimilarityForDoc5ComparedToDoc6() {
		Assert.assertFalse(candidates(DOC5, DOC6));
		Assert.assertEquals(0.5312, sim(DOC5, DOC6), delta);
		Assert.assertEquals(30, dist(DOC5, DOC6));
	}
	
	@Test
	public void approveSimilarityForDoc4ComparedToDoc6() {
		Assert.assertFalse(candidates(DOC4, DOC6));
		Assert.assertEquals(0.5312, sim(DOC4, DOC6), delta);
		Assert.assertEquals(30, dist(DOC4, DOC6));
	}
	
	@Test
	public void approveLengthOfFingerprintInBits() {
		int expected = 4; //4 ints with 2 byte per int = 8*8 bits
		int actual = fingerprinter.fingerprint(DOC1).size();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveCandidateGroups() {
		List<String> actual = new ArrayList<>();

		for(int i=0; i<DOCS.size(); i++) {
			for(int j=i+1; j<DOCS.size(); j++) {
				String message = candidates(DOCS.get(i), DOCS.get(j)) ? "are candidates" : " no candidates";
				
				actual.add( DOCS.get(i).getId() + " <-> " + DOCS.get(j).getId() + ": " + message);
			}
		}
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveAllFingerprints() {
		List<String> actualHashes = DOCS.stream()
				.map(i -> i.getId() + ": " + fingerprinter.fingerprint(i))
				.collect(Collectors.toList());
		
		Approvals.verifyAsJson(actualHashes);
	}
	
	private boolean candidates(CollectionDocument a, CollectionDocument b) {
		List<Integer> hashOfA = fingerprinter.fingerprint(a);
		List<Integer> hashOfB = fingerprinter.fingerprint(b);
		
		return hashOfA != null && hashOfB != null && hashOfA.stream().anyMatch(i -> hashOfB.contains(i));
	}

	private int dist(CollectionDocument a, CollectionDocument b) {
		byte[] aArr = HashTransformationUtil.integersToHash(fingerprinter.fingerprint(a));
		byte[] bArr = HashTransformationUtil.integersToHash(fingerprinter.fingerprint(b));
		
		return Hash.getHammingDistance(aArr, bArr);
	}
	
	private double sim(CollectionDocument a, CollectionDocument b) {
		return fingerprinter.similarity(fingerprinter.fingerprint(a), fingerprinter.fingerprint(b));
	}
	
	private static CollectionDocument doc(int id, String fullyCanonicalizedContent) {
		return new CollectionDocument("doc" + id, null, fullyCanonicalizedContent, null, null, null);
	};
}
