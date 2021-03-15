package de.webis.copycat_spark.app;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.copycat_spark.app.SampleNearDuplicates.NearDuplicate;
import de.webis.copycat_spark.spark.SparkRelevanceTransferDataConstruction;
import de.webis.copycat_spark.spark.eval.SparkAggregateKnowledgeTransferBetweenCrawls;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class FilterNearDuplicatesBetweenCorpora {
	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
			
			keepNearDuplicatesBetweenCorpora(input)
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT) + "/all", BZip2Codec.class);
			
			keepNearDuplicatesWithJudgmentInWebTrack(keepNearDuplicatesBetweenCorpora(input))
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT) + "/with-judgments-in-web-track", BZip2Codec.class);
		}
	}
	
	static JavaRDD<String> keepNearDuplicatesBetweenCorpora(JavaRDD<String> ret) {
		return ret.filter(i -> keepNearDuplicatesBetweenCorpora(i));
	}
	
	static JavaRDD<String> keepNearDuplicatesWithJudgmentInWebTrack(JavaRDD<String> ret) {
		return ret.filter(i -> keepNearDuplicatesWithJudgmentInWebTrack(i));
	}
	
	private static boolean keepNearDuplicatesWithJudgmentInWebTrack(String i) {
		NearDuplicate nd = NearDuplicate.fromString(i);
		
		return keepNearDuplicatesBetweenCorpora(i) && 
				(
						!SparkRelevanceTransferDataConstruction.possibleRelevanceTransfersFromTo(nd.getFirstId(), nd.getSecondId(), 0).isEmpty()
					||
						!SparkRelevanceTransferDataConstruction.possibleRelevanceTransfersFromTo(nd.getSecondId(), nd.getFirstId(), 0).isEmpty()
				);
	}
	
	private static boolean keepNearDuplicatesBetweenCorpora(String i) {
		NearDuplicate nd = NearDuplicate.fromString(i);
		
		return !SparkAggregateKnowledgeTransferBetweenCrawls.internalLabels(nd.getFirstId(), nd.getSecondId()).isEmpty();
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": FilterNearDuplicatesBetweenCorpora");

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
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": FilterNearDuplicatesBetweenCorpora")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
				"The input.");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The output.");

		return ret;
	}
}
