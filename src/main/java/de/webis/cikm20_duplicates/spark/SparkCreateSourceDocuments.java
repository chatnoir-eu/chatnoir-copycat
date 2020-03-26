package de.webis.cikm20_duplicates.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.cikm20_duplicates.util.SourceDocuments;
import de.webis.cikm20_duplicates.util.SourceDocuments.CollectionDocumentWithTopics;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkCreateSourceDocuments {
	
	private static final Map<String, Set<String>> DOCS_TO_TOPIC = docsToTopic();

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			transformAllImportantDocuments(context,
				new AnseriniCollectionReader(TrecCollections.CLUEWEB09),
				new AnseriniCollectionReader(TrecCollections.CLUEWEB12)
			).saveAsTextFile("cikm2020/source-documents");
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/source-documents");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<CollectionDocumentWithTopics> transformAllImportantDocuments(JavaSparkContext context, AnseriniCollectionReader<?>...acrs) {
		JavaRDD<CollectionDocumentWithTopics> ret = context.parallelize(Arrays.asList());
		
		for(AnseriniCollectionReader<?> acr: acrs) {
			ret = ret.union(transform(context, acr));
		}
		
		return ret;
	}
	
	private static JavaRDD<CollectionDocumentWithTopics> transform(JavaSparkContext context, AnseriniCollectionReader<?> acr) {
		return context.parallelize(acr.segmentPaths())
				.flatMap(s -> acr.collectionDocumentsInPath(s))
				.map(doc -> transformDocIfImportantOrNull(doc))
				.filter(i -> i != null);
	}
	
	private static CollectionDocumentWithTopics transformDocIfImportantOrNull(CollectionDocument doc) {
		if(!DOCS_TO_TOPIC.containsKey(doc.getId())) {
			return null;
		}
		
		return new CollectionDocumentWithTopics(doc, new ArrayList<>(DOCS_TO_TOPIC.get(doc.getId())));
	}
	
	private static Map<String, Set<String>> docsToTopic() {
		Map<String, Set<String>> ret = new HashMap<>();
		for(Map.Entry<String, Set<String>> topicToIds : SourceDocuments.topicsToDocumentIds().entrySet()) {
			String topic = topicToIds.getKey();
			for(String docId: topicToIds.getValue()) {
				if(!ret.containsKey(docId)) {
					ret.put(docId, new HashSet<>());
				}
				
				ret.get(docId).add(topic);
			}
		}
		
		return ret;
	}
}
