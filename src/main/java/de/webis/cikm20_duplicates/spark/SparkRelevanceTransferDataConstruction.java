package de.webis.cikm20_duplicates.spark;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.cikm20_duplicates.util.SourceDocuments;
import de.webis.cikm20_duplicates.util.SourceDocuments.SourceDocument;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import de.webis.WebisUUID;

public class SparkRelevanceTransferDataConstruction {

	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> exactDuplicates = context.textFile("cikm2020/exact-duplicates-simhash-cw09-cw12");
			JavaRDD<String> pairs = context.textFile("cikm2020/results/test-01");
		
			transfer(exactDuplicates, pairs).saveAsTextFile("cikm2020/relevance-transfer-pairs.jsonl");
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/relevance-transfer-data");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> transfer(JavaRDD<String> exactDuplicates, JavaRDD<String> pairs) {
		JavaRDD<String> duplicatePairs = exactDuplicates.flatMap(i -> SparkCountEdgeLabels.extractAllPairsWithJudgedDocuments(SparkCountEdgeLabels.docs(i)))
			.filter(i -> i != null)
			.map(i -> i._1() +"," + i._2() +"," + i._3())
			.distinct();
		
		return pairs.union(duplicatePairs).distinct()
				.flatMap(i -> bla(i));
	}

	private static Iterator<String> bla(String src) {
		String[] parsed = src.split(",");
		if(parsed.length != 3) {
			throw new RuntimeException("Can not transform input to tuple: '" + src + "'");
		}
		
		String leftId = parsed[0];
		String rightId = parsed[1];
		Integer k = Integer.parseInt(parsed[2]);
		
		List<String> ret = possibleRelevanceTransfersFromTo(leftId, rightId, k);
		ret.addAll(possibleRelevanceTransfersFromTo(rightId, leftId, k));
		
		return ret.iterator();
	}
	
	@SuppressWarnings("unchecked")
	private static List<String> possibleRelevanceTransfersFromTo(String src, String target, int k) {
		Set<String> sourceTopics = SparkCreateSourceDocuments.DOCS_TO_TOPIC.getOrDefault(src, Collections.EMPTY_SET);
		Set<String> targetTopics = SparkCreateSourceDocuments.DOCS_TO_TOPIC.getOrDefault(target, Collections.EMPTY_SET);
		List<RelevanceTransferPair> ret = new LinkedList<>();
		
		for(String sourceTopic: sourceTopics) {
			if(!targetTopics.contains(sourceTopic)) {
				ret.add(RelevanceTransferPair.transferPair(src, target, sourceTopic, k));
			}
		}
		
		return ret.stream().filter(i -> i!= null)
				.map(i -> i.toString())
				.collect(Collectors.toList());
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RelevanceTransferPair {
		private String srcId, targetId, topic,
			srcURL, targetURL;
		private int k, relevanceLabel;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
		
		public static RelevanceTransferPair transferPair(String src, String target, String topic, int k) {
			Map<String, SourceDocument> docIdToSrc = SourceDocuments.TOPIC_TO_ID_TO_SOURCE_DOC.get(topic);
			if(docIdToSrc == null || docIdToSrc.get(src) == null) {
				return null;
			}
			
			int relevanceLabel = docIdToSrc.get(src).getJudgment();
			
			return new RelevanceTransferPair(src, target, topic, chatNoirURL(src), chatNoirURL(target), k, relevanceLabel);
		}
		
		private static String webisUUID(String documentId) {
			return new WebisUUID(longChatNoirId(documentId))
					.generateUUID(documentId).toString();
		}
		
		private static String longChatNoirId(String documentId) {
			if(documentId.startsWith("clueweb09")) {
				return "clueweb09";
			} else if (documentId.startsWith("clueweb12")) {
				return "clueweb12";
			}
			
			throw new RuntimeException("ID '" + documentId + "' is not supported.");
		}
		
		private static String shortChatNoirId(String documentId) {
			if (documentId.startsWith("clueweb09")) {
				return "cw09";
			} else if (documentId.startsWith("clueweb12")) {
				return "cw12";
			}
			
			throw new RuntimeException("ID '" + documentId + "' is not supported.");
		}
		
		private static String chatNoirURL(String documentId) {
			return "https://chatnoir.eu/cache?uuid=" + webisUUID(documentId) + "&index="+ shortChatNoirId(documentId) +"&raw";
		}
	}
}
