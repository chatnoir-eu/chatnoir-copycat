package de.webis.cikm20_duplicates.app;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates;
import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates.DeduplicationStrategy;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class CreateDeduplicationCandidates {

	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			DeduplicationStrategy deduplicationStrategy = DeduplicationStrategy.productionDeduplication(50000);
			String output = parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT) +"/";
			
			JavaRDD<String> input = context.textFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
			
			SparkCreateDeduplicationCandidates.removedDocuments(input)
				.saveAsTextFile(output + "removed-documents", BZip2Codec.class);
			
			SparkCreateDeduplicationCandidates.exactDuplicates(input, deduplicationStrategy)
				.saveAsTextFile(output + "exact-duplicates", BZip2Codec.class);
			
			SparkCreateDeduplicationCandidates.createDeduplicationtasks(input, deduplicationStrategy)
				.saveAsTextFile(output + "near-duplicate-tasks", BZip2Codec.class);
		}
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": CreateDeduplicationCandidates");

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
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": CreateDeduplicationCandidates")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
				"The input path that is passed to JavaSparkContext.hadoopFile to extract Documents from warc files. E.g. 's3a://corpus-clueweb09'.");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The resulting document representations are stored under this location.");

		return ret;
	}
}
