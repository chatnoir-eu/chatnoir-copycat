package de.webis.cikm20_duplicates.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.cikm20_duplicates.util.SourceDocuments;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import scala.Tuple2;

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
	
	public static JavaRDD<Object> transformAllImportantDocuments(JavaSparkContext context, AnseriniCollectionReader<?>...acrs) {
		JavaRDD<Object> ret = context.parallelize(Arrays.asList());
		
		for(AnseriniCollectionReader<?> acr: acrs) {
			ret = ret.union(transform(context, acr));
		}
		
		return ret;
	}
	
	private static JavaRDD<Object> transform(JavaSparkContext context, AnseriniCollectionReader<?> acr) {
		return context.parallelize(acr.segmentPaths())
				.flatMap(s -> acr.collectionDocumentsInPath(s))
				.map(doc -> transformDocIfImportantOrNull(doc))
				.filter(i -> i != null)
				.map(i -> (Object) i);
	}
	
	private static Tuple2<CollectionDocument, List<String>> transformDocIfImportantOrNull(CollectionDocument doc) {
		if(!DOCS_TO_TOPIC.containsKey(doc.getId())) {
			return null;
		}
		
		return new Tuple2<>(doc, new ArrayList<>(DOCS_TO_TOPIC.get(doc.getId())));
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
