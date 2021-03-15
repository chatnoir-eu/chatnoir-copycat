package de.webis.copycat_spark.corpora;

import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class CollectionParsingIntegrationTest {
	@Test
	public void clueWeb09Sample() {
		CollectionConfiguration collection = Util.collectionConfigurationWithSharedTasks("src/test/resources/data/clueweb-09-sample", TrecCollections.CLUEWEB09);
		List<CollectionDocument> actual = docsInSample(collection);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void clueWeb12Sample() {
		CollectionConfiguration collection = Util.collectionConfigurationWithSharedTasks("src/test/resources/data/clueweb-12-sample", TrecCollections.CLUEWEB12);
		List<CollectionDocument> actual = docsInSample(collection);
		
		Approvals.verifyAsJson(actual);
	}
	
	private static List<CollectionDocument> docsInSample(CollectionConfiguration collection) {
		AnseriniCollectionReader<?> reader = new AnseriniCollectionReader<>(collection);
		
		return reader.extractRawDocumentsFromCollection();
	}
}
