package de.webis.cikm20_duplicates.spark.eval;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.cikm20_duplicates.spark.SparkEnrichRelevanceTransferPairs;
import de.webis.cikm20_duplicates.spark.SparkCanonicalLinkGraphExtraction.CanonicalLinkGraphEdge;
import de.webis.cikm20_duplicates.spark.eval.SparkEvaluateSimHashFeatures.FeatureSetCandidate;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkCalculatePrecisionInCanonicalLinkGraph {

	private static final String DIR = "cikm2020/canonical-link-graph/";
	
	private static final String[] CORPORA = new String[] {/*"cw09", "cw12",*/ "cc-2015-11"};
	
	public static void main(String[] args) {
		String corpus = CORPORA[0];
		String docs_dir = DIR + corpus + "-sample-0.1-and-large-groups";
		
		try(JavaSparkContext jsc = context()) {
			for(String feature: featureNames()) {
				List<FeatureSetCandidate> candidatesForFeature = jsc.textFile(DIR + corpus + "-candidates-for-feature-set-hash-evaluation")
						.map(i -> FeatureSetCandidate.fromString(i))
						.filter(i -> feature.equals(i.getFeatureName()))
						.takeSample(false, 50000);
				
				Set<String> idsToKeep = idsToKeep(candidatesForFeature);
				JavaPairRDD<String, CollectionDocument> docs = docs(jsc, docs_dir, idsToKeep);
				JavaRDD<TwoDocsForFeatureWithS3Score> rdd = jsc.parallelize(candidatesForFeature, 500)
						.map(i -> new TwoDocsForFeatureWithS3Score(i, null, null, 0.0));
				
				rdd = leftJoin(rdd, docs);
				rdd = rightJoin(rdd, docs);
				
				rdd.map(i -> addS3Score(i))
					.map(i -> i.toString())
					.saveAsTextFile(DIR + "feature-set-precision-experiments/" + corpus + "-" + feature + "-raw-data.jsonl");
			}
		}
	}
	
	private static TwoDocsForFeatureWithS3Score addS3Score(TwoDocsForFeatureWithS3Score i) {
		i.setS3Score(SparkEnrichRelevanceTransferPairs.s3Score(i.leftDoc, i.rightDoc));
		return i;
	}
	
	private static JavaRDD<TwoDocsForFeatureWithS3Score> leftJoin(JavaRDD<TwoDocsForFeatureWithS3Score> rdd, JavaPairRDD<String, CollectionDocument> docs) {
		JavaPairRDD<String, TwoDocsForFeatureWithS3Score> ret = rdd.mapToPair(i -> new Tuple2<>(i.leftDoc.getId(), i));
		
		return ret.join(docs).map(i -> {
			TwoDocsForFeatureWithS3Score iNew = i._2()._1();
			iNew.setLeftDoc(i._2()._2());
			
			return iNew;
		});
	}
	
	
	private static JavaRDD<TwoDocsForFeatureWithS3Score> rightJoin(JavaRDD<TwoDocsForFeatureWithS3Score> rdd, JavaPairRDD<String, CollectionDocument> docs) {
		JavaPairRDD<String, TwoDocsForFeatureWithS3Score> ret = rdd.mapToPair(i -> new Tuple2<>(i.rightDoc.getId(), i));
		
		return ret.join(docs).map(i -> {
			TwoDocsForFeatureWithS3Score iNew = i._2()._1();
			iNew.setRightDoc(i._2()._2());
			
			return iNew;
		});
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TwoDocsForFeatureWithS3Score {
		private FeatureSetCandidate candidate;
		private CollectionDocument leftDoc;
		private CollectionDocument rightDoc;
		private double s3Score;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
	}
	
	private static Set<String> idsToKeep(List<FeatureSetCandidate> candidatesForFeature) {
		Set<String> ret = new HashSet<>(candidatesForFeature.size() * 2);
		
		for(FeatureSetCandidate i: candidatesForFeature) {
			ret.add(i.getFirstId());
			ret.add(i.getSecondId());
		}
		
		return ret;
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/precision-of-simhash-features");
		
		return new JavaSparkContext(conf);
	}
	
	private static JavaPairRDD<String, CollectionDocument> docs(JavaSparkContext jsc, String dir, Set<String> idsToKeep) {
		return jsc.textFile(dir).map(i -> docOrNull(i, idsToKeep))
				.filter(i -> i != null)
				.mapToPair(i -> new Tuple2<>(i.getId(), i));
				
	}
	
	private static CollectionDocument docOrNull(String src, Set<String> idsToKeep) {
		if(src == null || idsToKeep == null) {
			return null;
		}
		
		CanonicalLinkGraphEdge ret = CanonicalLinkGraphEdge.fromString(src);
		
		if(ret == null || ret.getDoc() == null || ret.getDoc().getId() == null || !idsToKeep.contains(ret.getDoc().getId())) {
			return null;
		}
		
		return ret.getDoc();
	}

	public static List<String> featureNames() {
		return new ArrayList<>(SparkEvaluateSimHashFeatures.allFeatures(new CollectionDocument("", "", "", null)).keySet());
	}
}
