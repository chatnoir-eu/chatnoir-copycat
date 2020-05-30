package de.webis.cikm20_duplicates.spark;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.ImmutableList;

import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates.DeduplicationUnit;
import de.webis.cikm20_duplicates.util.ClientLocalDeduplication.DeduplicationTask;
import de.webis.cikm20_duplicates.util.SourceDocuments.DocumentWithFingerprint;
import scala.Tuple2;

public class SparkCreateCanonicalLinkDeduplicationTasks {
	
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			for(String corpus: new String[] {"cw12", "cc-2015-11", "cc-2017-04"}) {
				JavaRDD<String> input = context.textFile(inputPath(corpus));
				
				urlDeduplicationTask(input)
					.saveAsTextFile(path(corpus) + "-near-duplicate-tasks");
			}
		}
	}
	
	public static String inputPath(String corpus) {
		if ("cw09-cw12".equals(corpus)) {
			return "cikm2020/document-fingerprints-final/cw*-jsonl.bzip2";
		} else if ("cw09-cw12-cc-2015-11".equals(corpus)) {
			return "cikm2020/document-fingerprints-final/{cw,cc-2015}*-jsonl.bzip2";
		}
		
		return "cikm2020/document-fingerprints-final/" + corpus +"-jsonl.bzip2";
	}
	
	public static String path(String corpus) {
		return "cikm2020/deduplication-final/64BitK3SimHashOneGramms-canonical-urls/" + corpus;
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/canonical-link-deduplication-tasks");

		return new JavaSparkContext(conf);
	}

	public static JavaRDD<String> urlDeduplicationTask(JavaRDD<String> input) {
		JavaRDD<DocumentWithFingerprint> docsWithCanonicalURL = input.map(i -> DocumentWithFingerprint.fromString(i))
				.filter(i -> i.getCanonicalURL() != null);
		
		JavaPairRDD<String, DeduplicationUnit> parsedInput = hashPartitionToDocument(docsWithCanonicalURL);
				
		return parsedInput.groupByKey()
				.map(i -> workingPackages(i._2()))
				.filter(i -> i != null);
	}

	private static String workingPackages(Iterable<DeduplicationUnit> bla) {
		List<DeduplicationUnit> task = new LinkedList<>(new HashSet<>(ImmutableList.copyOf(bla)));
		
		if(task.size() <= 1) {
			return null;
		}
		
		return new DeduplicationTask(task).toString();
	}

	private static JavaPairRDD<String, DeduplicationUnit> hashPartitionToDocument(JavaRDD<DocumentWithFingerprint> docsWithCanonicalURL) {
		return docsWithCanonicalURL
				.flatMapToPair(doc -> extractHashesToDocId(doc));
	}

	private static Iterator<Tuple2<String, DeduplicationUnit>> extractHashesToDocId(DocumentWithFingerprint doc) {
		List<Tuple2<String, DeduplicationUnit>> ret = new ArrayList<>();
		DeduplicationUnit dedupUnit = new DeduplicationUnit(doc.getDocId(), doc.getFingerprints().get("64BitK3SimHashOneGramms"));
		
		for(Integer hashPart: dedupUnit.getHashParts()) {
			ret.add(new Tuple2<>(doc.getCanonicalURL().hashCode() + "-" + hashPart, dedupUnit));
		}
		
		return ret.iterator();
	}
}
