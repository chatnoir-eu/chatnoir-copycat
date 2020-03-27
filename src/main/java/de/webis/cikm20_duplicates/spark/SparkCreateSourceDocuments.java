package de.webis.cikm20_duplicates.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import de.webis.cikm20_duplicates.util.FingerPrintUtil;
import de.webis.cikm20_duplicates.util.SourceDocuments;
import de.webis.cikm20_duplicates.util.SourceDocuments.CollectionDocumentWithTopics;
import de.webis.cikm20_duplicates.util.SourceDocuments.DocumentWithFingerprint;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import scala.Tuple2;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkCreateSourceDocuments {
	
	static final Map<String, Set<String>> DOCS_TO_TOPIC = docsToTopic();
	
	private static final AnseriniCollectionReader<?>
			CLUEWEB09 = new AnseriniCollectionReader<>(TrecCollections.CLUEWEB09),
			CLUEWEB12 = new AnseriniCollectionReader<>(TrecCollections.CLUEWEB12);

	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
//			transformAllImportantDocuments(context, CLUEWEB09, CLUEWEB12)
//				.saveAsTextFile("cikm2020/source-documents");
			fingerprintAllDocuments(context, CLUEWEB09, CLUEWEB12)
				.saveAsTextFile("cikm2020/document-fingerprints");
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/source-documents");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<CollectionDocumentWithTopics> transformAllImportantDocuments(JavaSparkContext context, AnseriniCollectionReader<?>...acrs) {
		return docs(context, acrs)
				.map(doc -> transformDocIfImportantOrNull(doc))
				.filter(i -> i != null); 
	}
	
	@SuppressWarnings("rawtypes")
	public static JavaRDD<DocumentWithFingerprint> fingerprintAllDocuments(JavaSparkContext context, AnseriniCollectionReader...acr) {
		Fingerprinter<Integer> fp = FingerPrintUtil.minHashFingerPrinting(1);
		
		return docs(context, acr)
				.map(i -> new DocumentWithFingerprint(i.getId(), fp.fingerprint(i)));
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static JavaRDD<CollectionDocument> docs(JavaSparkContext context, AnseriniCollectionReader...acrs) {
		List<Tuple2<AnseriniCollectionReader, String>> readerToSegment = new ArrayList<>();
		
		for(AnseriniCollectionReader acr: acrs) {
			List<String> segmentPaths = acr.segmentPaths();
			for(String segmentPath: segmentPaths) {
				readerToSegment.add(new Tuple2<>(acr, segmentPath));
			}
		}
		
		return context.parallelize(readerToSegment, readerToSegment.size())
				.flatMap(i -> i._1().collectionDocumentsInPath(i._2()));
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
