package de.webis.cikm20_duplicates.app;

import java.net.URL;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import co.nstant.in.cbor.model.Map;
import de.webis.cikm20_duplicates.spark.SparkEnrichRelevanceTransferPairs;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil;
import de.webis.cikm20_duplicates.util.TakeRandom;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
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
			Set<Tuple3<String, String, Integer>> sample = new HashSet<>();
			
			for(int k=0; k<4; k++) {
				JavaRDD<Tuple3<String, String, Integer>> nearDuplicatesAtDistanceK = nearDuplicatePairsForK(parsedArgs, context, k);
				List<Tuple3<String, String, Integer>> iter = nearDuplicatesAtDistanceK.takeSample(false, 3* parsedArgs.getInt(ArgumentParsingUtil.ARG_NUM));
				List<Tuple3<String, String, Integer>> currentSample = TakeRandom.takeRandomElements(parsedArgs.getInt(ArgumentParsingUtil.ARG_NUM), iter);
				
				sample.addAll(currentSample);
			}
			
			List<String> ret = sample.stream().map(i -> samplePairToString(i, parsedArgs)).collect(Collectors.toList());
			
			context.parallelize(ret).repartition(1)
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}

	@SneakyThrows
	static String samplePairToString(Tuple3<String, String, Integer> i, Namespace parsedArgs) {
		java.util.Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("firstId", i._1());
		ret.put("secondId", i._2());
		ret.put("hemmingDistance", i._3());
		
		URL firstURL = new URL(CollectionDocumentUtil.chatNoirURL(parsedArgs.getString( ArgumentParsingUtil.UUID_PREFIX), i._1(), parsedArgs.getString( ArgumentParsingUtil.UUID_INDEX)));
		URL secondURL = new URL(CollectionDocumentUtil.chatNoirURL(parsedArgs.getString( ArgumentParsingUtil.UUID_PREFIX), i._2(), parsedArgs.getString( ArgumentParsingUtil.UUID_INDEX)));
		
		ret.put("firstURL", firstURL);
		ret.put("secondURL", secondURL);
		
		CollectionDocument firstDocument = CollectionDocumentUtil.loadCollectionDocument(i._1(), firstURL);
		CollectionDocument secondDocument = CollectionDocumentUtil.loadCollectionDocument(i._2(), secondURL);

		ret.put("firstDocument", firstDocument);
		ret.put("secondDocument", secondDocument);
		
		ret.put("s3Score", SparkEnrichRelevanceTransferPairs.s3Score(firstDocument,secondDocument));
		
		return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(ret);
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
	@SuppressWarnings("unchecked")
	private static Tuple3<String, String, Integer> parseExactDuplicates(String src) {
		try {
			java.util.Map<String, Object> ret = (java.util.Map<String, Object>) new com.fasterxml.jackson.databind.ObjectMapper().readValue(src, Map.class);
			List<String> ids = (List<String>) ret.get("equivalentDocuments");

			return new Tuple3<>(ids.get(0), ids.get(1), 0);
		} catch(Exception e) {
			System.out.println("---> '" + src +"'");
			return null;
		}
	}

	@SneakyThrows
	@SuppressWarnings({ "unchecked", "unused" })
	private static Tuple3<String, String, Integer> parseNearDuplicates(String src) {
		try {
			java.util.Map<String, Object> ret = (java.util.Map<String, Object>) new com.fasterxml.jackson.databind.ObjectMapper()
					.readValue(src, Map.class);
	
			return new Tuple3<>((String) ret.get("firstId"), (String) ret.get("secondId"),
					(Integer) ret.get("hemmingDistance"));
		} catch(Exception e) {
			System.out.println("---> '" + src +"'");
			return null;
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
