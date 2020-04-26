package de.webis.cikm20_duplicates.spark;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.aitools.ir.fingerprinting.representation.HashVector;
import de.webis.cikm20_duplicates.spark.SparkRelevanceTransferDataConstruction.RelevanceTransferPair;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil;
import de.webis.cikm20_duplicates.util.FingerPrintUtil;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.SneakyThrows;

public class SparkEnrichRelevanceTransferPairs {

	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile("cikm2020/relevance-transfer-pairs.jsonl");
		
			enrich(input).saveAsTextFile("cikm2020/enriched-relevance-transfer-pairs.jsonl");
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/enrich-relevance-transfer-data");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> enrich(JavaRDD<String> input) {
		return input.map(i -> RelevanceTransferPair.fromString(i))
				.map(i -> enrich(i));
	}

	@SneakyThrows
	private static String enrich(RelevanceTransferPair pair) {
		CollectionDocument srcDoc = CollectionDocumentUtil.loadCollectionDocument(pair.getSrcId());
		CollectionDocument targetDoc = CollectionDocumentUtil.loadCollectionDocument(pair.getTargetId());
		HashVector srcVec = FingerPrintUtil.internalHashVector(srcDoc, 64);
		HashVector targetVec = FingerPrintUtil.internalHashVector(targetDoc, 64);
		
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("srcId", pair.getSrcId());
		ret.put("targetId", pair.getTargetId());
		ret.put("topic", pair.getTopic());
		ret.put("relevanceLabel", pair.getRelevanceLabel());
		ret.put("srcURL", pair.getSrcURL());
		ret.put("targetURL", pair.getTargetURL());
		ret.put("k", pair.getK());
		ret.put("cosine-similarity", srcVec.getCosSimilarity(targetVec));
//		ret.put("jaccard", value);
		
		return new ObjectMapper().writeValueAsString(ret);
	}
}
