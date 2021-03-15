package de.webis.copycat_spark.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.webis.copycat_spark.util.FingerPrintUtil;
import de.webis.copycat_spark.util.FingerPrintUtil.Fingerprinter;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class MinHashFingerprintingTest {
	private static final CollectionDocument
		DOC1 = doc("doc-1", "a b c d e f g h i j k"),
		DOC2 = doc("doc-2", "a b c d e f g h j k i"),
		DOC3 = doc("doc-3", "a b c d e f g h j k j"),
		DOC4 = doc("doc-4", "k b c d e f g h j k k"),
		DOC5 = doc("doc-5", "a a a a a a a a a a a"),
		DOC6 = doc("doc-6", "a a a a b a a b a a a"),
		DOC7 = doc("doc-7", "a a a a a a a b a a a"),
		DOC8 = doc("doc-8", "a a a a a a a b a a b");
	
	private static List<CollectionDocument> DOCS = Arrays.asList(
		DOC1, DOC2, DOC3, DOC4, DOC5, DOC6, DOC7, DOC8
	);
	
	private Fingerprinter<Integer> fingerprinter;
	
	private final double delta = 1e-3;
	
	@Before
	public void setUp() {
		long seed = 1;
		fingerprinter = FingerPrintUtil.minHashFingerPrinting(seed);
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
		Assert.assertEquals(0.416, sim(DOC1, DOC2), delta);
	}
	
	@Test
	public void approveSimilarityForDoc1ComparedToDoc6() {
		Assert.assertFalse(candidates(DOC1, DOC6));
		Assert.assertEquals(0.0, sim(DOC1, DOC6), delta);
	}
	
	@Test
	public void approveSimilarityForDoc2ComparedToDoc3() {
		Assert.assertTrue(candidates(DOC2, DOC3));
		Assert.assertEquals(0.5, sim(DOC2, DOC3), delta);
	}
	
	@Test
	public void approveExampleFalseNegatives() {
		//As playground: here are some false positives that are known. As both share "b c d e f g h j k" 
		Assert.assertFalse(candidates(DOC2, DOC4));
		Assert.assertEquals(0.0, sim(DOC2, DOC4), delta);
		
		Assert.assertFalse(candidates(doc("doc-5", "a a a a a a a a a a a"), doc("doc-5", "a a a a a a a a a a b")));
		Assert.assertEquals(0.0, sim(doc("doc-5", "a a a a a a a a a a a"), doc("doc-5", "a a a a a a a a a a b")), delta);
		
		Assert.assertFalse(candidates(doc("doc-5", "a a a a a a a a a a a"), doc("doc-5", "b a a a a a a a a a b")));
		Assert.assertEquals(0.0, sim(doc("doc-5", "a a a a a a a a a a a"), doc("doc-5", "b a a a a a a a a a b")), delta);
	}
	
	@Test
	public void approveSimilarityForDoc5ComparedToDoc6() {
		Assert.assertFalse(candidates(DOC5, DOC6));
		Assert.assertEquals(0.0, sim(DOC5, DOC6), delta);
	}

	@Test
	public void approveLengthOfFingerprintInBits() {
		int expected = 12; //12 ints = 384 bits as in Henzinger et al. (12*8*4)
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

	private double sim(CollectionDocument a, CollectionDocument b) {
		return fingerprinter.similarity(fingerprinter.fingerprint(a), fingerprinter.fingerprint(b));
	}
	
	private static CollectionDocument doc(String docId, String fullyCanonicalizedContent) {
		return new CollectionDocument(docId, null, fullyCanonicalizedContent, null, null, null);
	};
}
