package de.webis.cikm20_duplicates.spark;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.ImmutableList;

import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates.DeduplicationUnit;
import de.webis.cikm20_duplicates.util.ClientLocalDeduplication.DeduplicationTask;
import de.webis.cikm20_duplicates.util.SourceDocuments.DocumentWithFingerprint;
import lombok.AllArgsConstructor;
import lombok.Data;
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
		return hashPartitionToDocument(input).groupByKey()
				.flatMap(i -> workingPackages2(i._2()));
	}

	private static Iterator<String> workingPackages2(Iterable<DeduplicationUnit2> bla) {
		List<DeduplicationUnit2> tasks = new LinkedList<>(new HashSet<>(ImmutableList.copyOf(bla)));
		Map<URL, List<DeduplicationUnit>> ret = new LinkedHashMap<>();
		
		for(DeduplicationUnit2 t: tasks) {
			if(!ret.containsKey(t.url)) {
				ret.put(t.url, new ArrayList<>());
			}
			
			ret.get(t.url).add(new DeduplicationUnit(t.id, t.hashParts));
		}
		
		return ret.values().stream()
				.filter(i -> i.size() > 1)
				.map(i -> new DeduplicationTask(i).toString())
				.iterator();
	}

	private static JavaPairRDD<Integer, DeduplicationUnit2> hashPartitionToDocument(JavaRDD<String> docsWithCanonicalURL) {
		return docsWithCanonicalURL
				.flatMapToPair(doc -> extractHashesToDocId(doc));
	}

	private static Iterator<Tuple2<Integer, DeduplicationUnit2>> extractHashesToDocId(String src) {
		DocumentWithFingerprint doc = DocumentWithFingerprint.fromString(src);
		List<Tuple2<Integer, DeduplicationUnit2>> ret = new ArrayList<>();
		
		if(doc.getCanonicalURL() == null ||doc.getCanonicalURL().toString().trim().isEmpty()) {
			return ret.iterator();
		}
		
		DeduplicationUnit2 dedupUnit = new DeduplicationUnit2(doc.getDocId(), doc.getCanonicalURL(), doc.getFingerprints().get("64BitK3SimHashOneGramms"));
		
		for(Integer hashPart: dedupUnit.getHashParts()) {
			ret.add(new Tuple2<>(hashPart, dedupUnit));
		}
		
		return ret.iterator();
	}
	
	@Data
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class DeduplicationUnit2 implements Serializable {
		private String id;
		private URL url;
		private ArrayList<Integer> hashParts;
	}
}