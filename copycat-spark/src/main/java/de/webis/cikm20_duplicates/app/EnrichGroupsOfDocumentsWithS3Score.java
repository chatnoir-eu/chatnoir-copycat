package de.webis.cikm20_duplicates.app;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;

import de.webis.cikm20_duplicates.util.TakeRandom;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

public class EnrichGroupsOfDocumentsWithS3Score {
	static Namespace validArgumentsOrNull(String[] args) {
		ArgumentParser parser = argParser();

		try {
			return parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			return null;
		}
	}
	
	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);
		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
			
			JavaRDD<ArrayList<String>> sampledDocumentGroups = input.map(i -> sampleFromGroup(i));
			
			sampledDocumentGroups.flatMap(i -> calculate_all_pairs_similarity(i))
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}
	
	private static Iterator<String> calculate_all_pairs_similarity(ArrayList<String> i) {
		if(i == null || i.size() < 2) {
			return Collections.emptyIterator();
		}
		Collections.sort(i);
		List<String> allPairs = new ArrayList<>();
		
		for(int a=0; a< i.size(); a++) {
			for(int b=a+1; b< i.size(); b++) {
				allPairs.add(i.get(a) + "," + i.get(b));
			}	
		}
		
		Tuple2<String, Iterable<String>> groupForFirstId = new Tuple2<>(i.get(0), allPairs);
		EnrichPairsOfDocumentsWithS3SCore.CALCULATE_ONLY_S3 = false;
		return EnrichSimHashNearDuplicatesWithS3Similarity.enrichS3Score(groupForFirstId, EnrichSimHashNearDuplicatesWithS3Similarity.docResolver(null), EnrichSimHashNearDuplicatesWithS3Similarity.Format.CSV_FORMAT);
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": EnrichGroupsOfDocumentsWithS3Score");

		return new JavaSparkContext(conf);
	}

	@SuppressWarnings("unchecked")
	private static ArrayList<String> sampleFromGroup(String i) {
		try {
			ArrayList<String> ret = new ObjectMapper().readValue(i, ArrayList.class);
			
			return new ArrayList<>(TakeRandom.takeRandomElements(50, ret));
		} catch(Exception e) {
			return new ArrayList<>();
		}
	}

	private static ArgumentParser argParser() {
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": EnrichGroupsOfDocumentsWithS3Score")
			.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
			"Near-duplicate file in jsonl format");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
			.help("The resulting jsonl enriched with similarity scores.");
		
		return ret;
	}
}