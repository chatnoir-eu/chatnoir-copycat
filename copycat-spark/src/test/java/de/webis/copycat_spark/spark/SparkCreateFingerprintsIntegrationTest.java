package de.webis.copycat_spark.spark;

import static de.webis.copycat_spark.spark.SparkCreateSourceDocumentsIntegrationTest.sorted;

import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.spark.SparkCreateSourceDocuments;
import de.webis.copycat_spark.spark.SparkCreateSourceDocumentsIntegrationTest.DummyAnseriniCollectionReader;
import de.webis.copycat_spark.util.FingerPrintUtil;
import de.webis.copycat_spark.util.FingerPrintUtil.Fingerprinter;
import de.webis.copycat_spark.util.SourceDocuments.DocumentWithFingerprint;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.collection.ClueWeb09Collection.Document;
import lombok.SneakyThrows;

public class SparkCreateFingerprintsIntegrationTest extends SparkIntegrationTestBase {
	private static final List<Fingerprinter<Integer>> FINGERPRINTERS = Arrays.asList(
			FingerPrintUtil.minHashFingerPrinting(1),
			FingerPrintUtil.simHashFingerPrinting(64, 3)
	);
	
	@Test
	public void testWithEmptyDocuments() {
		AnseriniCollectionReader<Document> acr = new DummyAnseriniCollectionReader();
		long expected = 0;
		
		JavaRDD<DocumentWithFingerprint> rdd = SparkCreateSourceDocuments.fingerprintAllDocuments(jsc(), FINGERPRINTERS, acr);
		List<String> actual = sorted(rdd);
		
		Assert.assertEquals(expected,actual.size());
	}
	
	@Test
	public void testWithSomeImportantAndSomeUnimportantDocuments() {
		AnseriniCollectionReader<Document> acr = new DummyAnseriniCollectionReader(
				doc("a b c d e f g h i j k"), doc("k b c d e f g h j k k"), 
				doc("a a a a a a a b a a b")
		);
		JavaRDD<DocumentWithFingerprint> rdd = SparkCreateSourceDocuments.fingerprintAllDocuments(jsc(), FINGERPRINTERS, acr);
		List<String> actual = sorted(rdd);

		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void testWithMultipleAcrs() {
		AnseriniCollectionReader<Document> acr1 = new DummyAnseriniCollectionReader(doc("a b c d e f g h i j k"));
		AnseriniCollectionReader<Document> acr2 = new DummyAnseriniCollectionReader(doc("k b c d e f g h j k k"));
		AnseriniCollectionReader<Document> acr3 = new DummyAnseriniCollectionReader(doc("a a a a a a a b a a b"));
		JavaRDD<DocumentWithFingerprint> rdd = SparkCreateSourceDocuments.fingerprintAllDocuments(jsc(), Arrays.asList(FingerPrintUtil.minHashFingerPrinting(1), FingerPrintUtil.simHashFingerPrinting(64, 3)), acr1, acr2, acr3);
		List<String> actual = sorted(rdd);

		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void testWithMultipleAcrsForProductionFingerprinters() {
		AnseriniCollectionReader<Document> acr1 = new DummyAnseriniCollectionReader(doc("a b c d e f g h i j k"));
		AnseriniCollectionReader<Document> acr2 = new DummyAnseriniCollectionReader(doc("k b c d e f g h j k k"));
		AnseriniCollectionReader<Document> acr3 = new DummyAnseriniCollectionReader(doc("a a a a a a a b a a b"));
		JavaRDD<DocumentWithFingerprint> rdd = SparkCreateSourceDocuments.fingerprintAllDocuments(jsc(), SparkCreateSourceDocuments.PRODUCTION_FINGERPRINTS, acr1, acr2, acr3);
		List<String> actual = sorted(rdd);

		Approvals.verifyAsJson(actual);
	}
	
	@SneakyThrows
	private static CollectionDocument doc(String canonicalizedContent) {
		return new CollectionDocument("id of " + canonicalizedContent, "full content of " + canonicalizedContent, canonicalizedContent, null, new URL("http://" + canonicalizedContent), null);
	}
}
