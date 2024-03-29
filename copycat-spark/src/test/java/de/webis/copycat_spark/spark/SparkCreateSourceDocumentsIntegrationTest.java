package de.webis.copycat_spark.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.spark.SparkCreateSourceDocuments;
import de.webis.copycat_spark.util.SourceDocuments.CollectionDocumentWithTopics;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.collection.ClueWeb09Collection.Document;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkCreateSourceDocumentsIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void testWithEmptyDocuments() {
		AnseriniCollectionReader<Document> acr = new DummyAnseriniCollectionReader();
		long expected = 0;
		
		JavaRDD<CollectionDocumentWithTopics> rdd = SparkCreateSourceDocuments.transformAllImportantDocuments(jsc(), acr);
		
		Assert.assertEquals(expected, rdd.count());
	}
	
	@Test
	public void testWithOnlyUnimportantDocuments() {
		AnseriniCollectionReader<Document> acr = new DummyAnseriniCollectionReader(
			doc("a"), doc("b"), doc("c")
		);
		long expected = 0;
		
		JavaRDD<CollectionDocumentWithTopics> rdd = SparkCreateSourceDocuments.transformAllImportantDocuments(jsc(), acr);
		
		Assert.assertEquals(expected, rdd.count());
	}
	
	@Test
	public void testWithSomeImportantAndSomeUnimportantDocuments() {
		AnseriniCollectionReader<Document> acr = new DummyAnseriniCollectionReader(
				doc("a"), doc("b"), 
				doc("clueweb09-en0008-02-29970"), // judged for topic 50 of the web track
				doc("c")
		);
		JavaRDD<CollectionDocumentWithTopics> rdd = SparkCreateSourceDocuments.transformAllImportantDocuments(jsc(), acr);
		List<String> actual = sorted(rdd);

		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void testMultipleAcrsWithSomeImportantAndSomeUnimportantDocuments() {
		AnseriniCollectionReader<Document> acr1 = new DummyAnseriniCollectionReader(
				doc("a"), doc("b"), 
				doc("clueweb09-en0008-02-29970"), // judged for topic 50 of the web track
				doc("c")
		);
		AnseriniCollectionReader<Document> acr2 = new DummyAnseriniCollectionReader(
				doc("a"), doc("b"), doc("c")
		);
		AnseriniCollectionReader<Document> acr3 = new DummyAnseriniCollectionReader(
				doc("d"), doc("b"), 
				doc("clueweb12-1800tw-04-16339") // judged for topic 60 of the session track 
		);
		
		JavaRDD<CollectionDocumentWithTopics> rdd = SparkCreateSourceDocuments.transformAllImportantDocuments(jsc(), acr1, acr2, acr3);
		List<String> actual = sorted(rdd);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveCC17Collections() {
		List<String> collections = SparkCreateSourceDocuments.cc17Collections();
		
		Approvals.verifyAsJson(collections);
	}
	
	@SuppressWarnings("serial")
	public static class DummyAnseriniCollectionReader extends AnseriniCollectionReader<Document> {

		private final ArrayList<CollectionDocument> docs;
		
		public DummyAnseriniCollectionReader(CollectionDocument...docs) {
			super(null);
			this.docs = new ArrayList<>(Arrays.asList(docs));
		}
		
		@Override
		public List<String> segmentPaths() {
			return Arrays.asList("a");
		}
		
		@Override
		public Iterator<CollectionDocument> collectionDocumentsInPath(String segmentPath) {
			return docs.iterator();
		}
	}
	
	private static CollectionDocument doc(String id) {
		return new CollectionDocument(id, "content of " + id, "fullyCanonicalizedContent of " + id, null, null, null);
	}
	
	public static List<String> sorted(JavaRDD<?> rdd) {
		List<String> ret = rdd.map(i -> i.toString()).collect();
		ret = new ArrayList<>(ret);
		Collections.sort(ret);
		
		return ret;
	}
}
