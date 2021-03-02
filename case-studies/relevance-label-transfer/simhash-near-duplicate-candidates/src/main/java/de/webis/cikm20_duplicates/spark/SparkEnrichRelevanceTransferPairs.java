package de.webis.cikm20_duplicates.spark;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.aitools.ir.fingerprinting.representation.HashVector;
import de.webis.cikm20_duplicates.spark.SparkRelevanceTransferDataConstruction.RelevanceTransferPair;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil;
import de.webis.cikm20_duplicates.util.FingerPrintUtil;
import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3Score;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3ScoreIntermediateResult;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.util.NGramms;
import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import lombok.SneakyThrows;

public class SparkEnrichRelevanceTransferPairs {
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<RelevanceTransferPair> input = context.textFile("cikm2020/relevance-transfer-pairs.jsonl")
					.map(i -> RelevanceTransferPair.fromString(i))
					.filter(i -> "clueweb09-en0002-28-35595".equals(i.getSrcId()));
		
			List<RelevanceTransferPair> tmp = input.take(1000);
			Collections.shuffle(tmp);
			
			List<String> enriched = tmp.subList(0,100).stream()
					.map(i -> enrich2(i))
					.collect(Collectors.toList());
			
			context.parallelize(enriched)
					.saveAsTextFile("cikm2020/enriched-relevance-transfer-pairs.jsonl");
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
		ret.put("s3-score", s3Score(srcDoc, targetDoc));
//		ret.put("jaccard", value);
		
		return new ObjectMapper().writeValueAsString(ret);
	}
	
	@SneakyThrows
	private static String enrich2(RelevanceTransferPair pair) {
		CollectionDocument srcDoc = CollectionDocumentUtil.loadCollectionDocument(pair.getSrcId());
		CollectionDocument targetDoc = CollectionDocumentUtil.loadCollectionDocument(pair.getTargetId());
		HashVector srcVec = FingerPrintUtil.internalHashVector(srcDoc, 64);
		HashVector targetVec = FingerPrintUtil.internalHashVector(targetDoc, 64);
		
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("src", srcDoc);
		ret.put("target", targetDoc);
		ret.put("srcId", pair.getSrcId());
		ret.put("targetId", pair.getTargetId());
		ret.put("topic", pair.getTopic());
		ret.put("relevanceLabel", pair.getRelevanceLabel());
		ret.put("srcURL", pair.getSrcURL() +"&plain");
		ret.put("targetURL", pair.getTargetURL() +"&plain");
		ret.put("k", pair.getK());
		ret.put("cosine-similarity", srcVec.getCosSimilarity(targetVec));
		ret.put("s3-score", s3Score(srcDoc, targetDoc));
//		ret.put("jaccard", value);
		
		return new ObjectMapper().writeValueAsString(ret);
	}
	
	public static double s3Score(CollectionDocument a, CollectionDocument b) {
		DocumentHash aHash = new DocumentHash(a);
		Set<Word8Gramm> aWord8Gramms = word8Gramms(a);
		DocumentHash bHash = new DocumentHash(b);
		Set<Word8Gramm> bWord8Gramms = word8Gramms(b);
		aWord8Gramms.retainAll(bWord8Gramms);
		
		S3ScoreIntermediateResult data = new S3ScoreIntermediateResult();
		data.setLeftMetadata(aHash);
		data.setRightMetadata(bHash);
		data.setCommonNGramms(aWord8Gramms.size());
		
		S3Score ret = new S3Score(data);
		
		return ret.getS3Score();
	}
	
	private static Set<Word8Gramm> word8Gramms(CollectionDocument doc) {
		List<Word8Gramm> tmp = NGramms.build8Gramms(doc.getFullyCanonicalizedContent());
		
		return new HashSet<Word8Gramm>(tmp);
	}
}
