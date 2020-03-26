package de.webis.cikm20_duplicates.spark;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.curator.shaded.com.google.common.collect.Iterators;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import de.webis.cikm20_duplicates.util.SourceDocuments.CollectionDocumentWithTopics;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import scala.Tuple2;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkCreateDeduplicationCandidates {

	public static JavaPairRDD<String, Iterable<Tuple2<String, CollectionDocument>>> topicsToImportantDocuments(JavaRDD<String> rdd) {
		JavaRDD<Tuple2<String, CollectionDocument>> ret = rdd
				.map(i -> CollectionDocumentWithTopics.fromString(i))
				.flatMap(i -> flattenTopicsForDoc(i));
 
		return ret.groupBy(i -> i._1());
	}

	private static Iterator<Tuple2<String, CollectionDocument>> flattenTopicsForDoc(CollectionDocumentWithTopics docWithTopics) {
		return docWithTopics.getTopics().stream()
				.map(topic -> new Tuple2<>(topic, docWithTopics.getDoc()))
				.iterator();
	}

	public static <T extends Comparable<T>> JavaRDD<Tuple2<String, Set<T>>> topicsToFingerPrintsOfImportantDocsPerTopic(JavaRDD<String> rdd, Fingerprinter<T> fingerprinter) {
		return topicsToImportantDocuments(rdd)
				.map(i -> new Tuple2<>(i._1, combineAll(i._2, fingerprinter)));
	}

	private static <T extends Comparable<T>> Set<T> combineAll(Iterable<Tuple2<String, CollectionDocument>> a, Fingerprinter<T> fingerprinter) {
		Set<T> ret = new HashSet<>();
		Iterator<CollectionDocument> docIterator = Iterators.transform(a.iterator(), i -> i._2());
		docIterator.forEachRemaining(doc -> ret.addAll(fingerprinter.fingerprint(doc)));

		return ret;
	}
}
