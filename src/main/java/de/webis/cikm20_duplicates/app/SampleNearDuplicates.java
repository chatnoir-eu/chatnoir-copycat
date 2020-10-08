package de.webis.cikm20_duplicates.app;

import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.aitools.ir.fingerprinting.representation.HashVector;
import de.aitools.ir.fingerprinting.representation.HashVectorSha3;
import de.webis.cikm20_duplicates.spark.SparkEnrichRelevanceTransferPairs;
import de.webis.cikm20_duplicates.spark.eval.SparkEvaluateSimHashFeatures;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil.HdfsMapFileDocumentResolver;
import de.webis.cikm20_duplicates.util.TakeRandom;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple3;

public class SampleNearDuplicates {
	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			String uuidPrefix = parsedArgs.getString(ArgumentParsingUtil.UUID_PREFIX);
			String uuidIndex = parsedArgs.getString(ArgumentParsingUtil.UUID_INDEX);
			List<String> ret = new ArrayList<>();
			
			for(int k=0; k<4; k++) {
				JavaRDD<Tuple3<String, String, Integer>> nearDuplicatesAtDistanceK = nearDuplicatePairsForK(parsedArgs, context, k);
				List<Tuple3<String, String, Integer>> iter = nearDuplicatesAtDistanceK.takeSample(false, 3* parsedArgs.getInt(ArgumentParsingUtil.ARG_NUM));
				List<Tuple3<String, String, Integer>> currentSample = TakeRandom.takeRandomElements(parsedArgs.getInt(ArgumentParsingUtil.ARG_NUM), iter);
				List<String> currentSampleMapped = context.parallelize(currentSample, currentSample.size()).map(i -> samplePairToString(i, uuidPrefix, uuidIndex)).collect();
				
				ret.addAll(currentSampleMapped);
			}
			
			context.parallelize(ret).repartition(1)
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}

	@SneakyThrows
	static String samplePairToString(Tuple3<String, String, Integer> i, String uuidPrefix, String uuidIndex) {
		java.util.Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("firstId", i._1());
		ret.put("secondId", i._2());
		ret.put("hemmingDistance", i._3());
		
		URL firstURL = new URL(CollectionDocumentUtil.chatNoirURL(uuidPrefix, i._1(), uuidIndex));
		URL secondURL = new URL(CollectionDocumentUtil.chatNoirURL(uuidPrefix, i._2(), uuidIndex));
		
		ret.put("firstURL", firstURL);
		ret.put("secondURL", secondURL);
		
		try {
			CollectionDocument firstDocument = new HdfsMapFileDocumentResolver(uuidIndex, uuidPrefix).loadCollectionDocument(i._1());
			CollectionDocument secondDocument = new HdfsMapFileDocumentResolver(uuidIndex, uuidPrefix).loadCollectionDocument(i._2());
	
			ret.put("firstDocument", firstDocument);
			ret.put("secondDocument", secondDocument);
			
			if(firstDocument != null && secondDocument != null) {
				ret.put("s3Score", SparkEnrichRelevanceTransferPairs.s3Score(firstDocument,secondDocument));
				
				HashVector aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.theeAndFiveGramms(firstDocument), 64);
				HashVector bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.theeAndFiveGramms(secondDocument), 64);

				ret.put("cosineSimilarity(3+5)", aVec.getCosSimilarity(bVec));

				aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(firstDocument, 8), 64);
				bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(secondDocument, 8), 64);

				ret.put("cosineSimilarity(8)", aVec.getCosSimilarity(bVec));
				
				aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(firstDocument, 1), 64);
				bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(secondDocument, 1), 64);

				ret.put("cosineSimilarity(1)", aVec.getCosSimilarity(bVec));
			}
		} catch(Exception e) {}
		
		return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(ret);
	}
	
	@Data
	@NoArgsConstructor
	public static class NdSample {
		private String firstId, secondId;
		private int hemmingDistance;
		private String firstURL, secondURL;
		private CollectionDocument firstDocument, secondDocument;
		private float s3Score;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(this);
		}
		
		@SneakyThrows
		public static NdSample fromString(String src) {
			return new com.fasterxml.jackson.databind.ObjectMapper().readValue(src, NdSample.class);
		}
	}
	
	private static JavaRDD<Tuple3<String, String, Integer>> nearDuplicatePairsForK(Namespace parsedArgs,
			JavaSparkContext jsc, int k) {
		if (k == 0) {
			return jsc.textFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT) + "-exact-duplicates")
					.map(i -> parseExactDuplicates(i))
					.filter(i -> i != null);
		} else {
			return jsc.textFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT) + "-near-duplicates")
					.filter(i -> i != null)
					.map(i -> parseNearDuplicates(i)).filter(i -> i._3() == k);
		}
	}

	@SneakyThrows
	static Tuple3<String, String, Integer> parseExactDuplicates(String src) {
		EquivalentDocs ret = new com.fasterxml.jackson.databind.ObjectMapper().readValue(src, EquivalentDocs.class);
			
		return new Tuple3<>(ret.getEquivalentDocuments().get(0), ret.getEquivalentDocuments().get(1), 0);
	}
	
	@Data
	@NoArgsConstructor
	public static class EquivalentDocs {
		private List<String> equivalentDocuments;
		private List<Integer> hash;
	}

	static Tuple3<String, String, Integer> parseNearDuplicates(String src) {
		NearDuplicate ret = NearDuplicate.fromString(src);
	
		return new Tuple3<>(ret.getFirstId(), ret.getSecondId(), ret.getHemmingDistance());
	}

	@Data
	@NoArgsConstructor
	public static class NearDuplicate {
		private String firstId, secondId;
		private int hemmingDistance;

		@SneakyThrows
		public static NearDuplicate fromString(String src) {
			return new com.fasterxml.jackson.databind.ObjectMapper().readValue(src, NearDuplicate.class);
		}
		
		@Override
		@SneakyThrows
		public String toString() {
			return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(this);
		}
	}

	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": SampleNearDuplicates");

		return new JavaSparkContext(conf);
	}

	static Namespace validArgumentsOrNull(String[] args) {
		ArgumentParser parser = argParser();

		try {
			return parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			return null;
		}
	}

	private static ArgumentParser argParser() {
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": SampleNearDuplicates")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
				"The prefix of the input path that contains the required directories (e.g. for `--"+ ArgumentParsingUtil.ARG_INPUT +" ecir2021/cw09-deduplication/min-length-10` it will use `ecir2021/cw09-deduplication/min-length-10-exact-duplicates` and `ecir2021/cw09-deduplication/min-length-10-near-duplicates`)");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The resulting sampled near-duplicates are stored under this location.");

		ret.addArgument("--" + ArgumentParsingUtil.ARG_NUM).required(Boolean.FALSE)
				.type(Integer.class)
				.setDefault(1000)
				.help("The number of samples per hamming distance.");
		
		ret.addArgument("--" + ArgumentParsingUtil.UUID_PREFIX).required(Boolean.TRUE)
				.type(String.class)
				.help("The uuid prefix that is used to calculate the chatnoir id of a document");
		
		ret.addArgument("--" + ArgumentParsingUtil.UUID_INDEX).required(Boolean.TRUE)
				.type(String.class)
				.help("The uuid index that is used to calculate the chatnoir id of a document");

		return ret;
	}
}
