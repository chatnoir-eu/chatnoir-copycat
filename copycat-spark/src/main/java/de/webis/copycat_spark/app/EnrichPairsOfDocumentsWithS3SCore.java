package de.webis.copycat_spark.app;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.aitools.ir.fingerprinting.representation.HashVector;
import de.aitools.ir.fingerprinting.representation.HashVectorSha3;
import de.webis.copycat_spark.spark.SparkEnrichRelevanceTransferPairs;
import de.webis.copycat_spark.spark.eval.SparkEvaluateSimHashFeatures;
import de.webis.copycat_spark.util.CollectionDocumentUtil;
import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import scala.Tuple2;

public class EnrichPairsOfDocumentsWithS3SCore {
	
	static boolean CALCULATE_ONLY_S3 = true;
	
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile("ecir2021/trec-judgments-in-wayback-machine/redirects-and-snapshots-as-collection-documents.jsonl");
			JavaRDD<TmpContent> tmp = input.map(i -> TmpContent.fromString(i));
			
			tmp.flatMap(i -> calculateSimilarity(i))
				.filter(i -> i != null)
				.repartition(1)
				.saveAsTextFile("file:///mnt/ceph/storage/data-in-progress/kibi9872/ecir2021/trec-judgments-in-wayback-machine/redirects-and-snapshots-with-similarity.jsonl");
		}
	}
	
	private static Iterator<String> calculateSimilarity(TmpContent i) {
		List<String> ret = new ArrayList<>();
		CollectionDocument firstDoc = doc(i.getTrecDocumentId());
		DocumentHash firstHash = new DocumentHash(firstDoc);
		Set<Word8Gramm> firstWord8Gramms = SparkEnrichRelevanceTransferPairs.word8Gramms(firstDoc);
		
		for(CollectionDocument secondDoc: i.getDocuments()) {
			if(CALCULATE_ONLY_S3) {
				ret.add(calculateSimilarity(firstHash, firstWord8Gramms, secondDoc));
			} else {
				ret.add(calculateSimilarity(firstDoc, secondDoc));
			}
		}
		
		return ret.iterator();
	}

	private static String calculateSimilarity(DocumentHash firstHash, Set<Word8Gramm> firstWord8Gramms, CollectionDocument secondDoc) {
		String  s3Score = "-1";
		
		if(firstHash != null && firstHash.getId() != null && firstWord8Gramms != null && secondDoc != null) {
			s3Score = String.format("%.4f", SparkEnrichRelevanceTransferPairs.s3Score(firstHash, firstWord8Gramms, secondDoc));
		}
		
		return "{\"firstId\":\"" + firstHash.getId() + "\",\"secondId\":\"" + secondDoc.getId() + "\",\"s3Score\":" +
			s3Score + "}";
	}
	
	private static String calculateSimilarity(CollectionDocument firstDoc, CollectionDocument secondDoc) {
		String  s3Score = "-1",
				cosineSimilarityOneGramms = "-1",
				cosineSimilarityEightGramms = "-1",
				cosineSimilarityThreeAndFiveGramms = "-1";
		
		if(firstDoc != null && secondDoc != null) {
			s3Score = String.format("%.4f", SparkEnrichRelevanceTransferPairs.s3Score(firstDoc, secondDoc));
			
			HashVector aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.theeAndFiveGramms(firstDoc), 64);
			HashVector bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.theeAndFiveGramms(secondDoc), 64);

			cosineSimilarityThreeAndFiveGramms = String.format("%.4f", aVec.getCosSimilarity(bVec));
			
			aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(firstDoc, 8), 64);
			bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(secondDoc, 8), 64);
			
			cosineSimilarityEightGramms = String.format("%.4f", aVec.getCosSimilarity(bVec));
			
			aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(firstDoc, 1), 64);
			bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(secondDoc, 1), 64);

			cosineSimilarityOneGramms = String.format("%.4f", aVec.getCosSimilarity(bVec));
		}
		
		return "{\"firstId\":\"" + firstDoc.getId() + "\",\"secondId\":\"" + secondDoc.getId() + "\",\"s3Score\":" +
			s3Score + ",\"cosineSimilarityOneGramms\":" + cosineSimilarityOneGramms + ",\"cosineSimilarityEightGramms\":"
			+ cosineSimilarityEightGramms +",\"cosineSimilarityThreeAndFiveGramms\":" + cosineSimilarityThreeAndFiveGramms + "}";
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class TmpContent implements Serializable {
		private String trecDocumentId;
		private List<CollectionDocument> documents;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
		
		@SneakyThrows
		public static TmpContent fromString(String src) {
			return new ObjectMapper().readValue(src, TmpContent.class);
		}

		public TmpContent(Tuple2<String, List<CollectionDocument>> i) {
			this(i._1(), i._2());
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("EnrichPairsOfDocumentsWithS3SCore");

		return new JavaSparkContext(conf);
	}
	
	private static CollectionDocument doc(String id) {
		return CollectionDocumentUtil.HdfsMapFileDocumentResolver.smartDocumentResolver().loadCollectionDocument(id);
	}
}
