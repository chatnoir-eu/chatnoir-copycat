package de.webis.cikm20_duplicates.app;

import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.archive.io.arc.ARCRecord;

import de.webis.cikm20_duplicates.util.arc.ARCParsingUtil;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class ArcUrlExtraction {
	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaPairRDD<LongWritable, ARCRecord> records = ARCParsingUtil.records(context, parsedArgs);
			JavaRDD<String> urls = records.map(i -> ARCParsingUtil.extractURL(i._2())).filter(i -> i != null);

			urls.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT));
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": ArcUrlExtraction");

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
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": ArcUrlExtraction")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
				"The input path that is passed to JavaSparkContext.hadoopFile to extract URLs from arc files. E.g. 's3a://corpus-commoncrawl-main-2008'.");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The resulting aggregations are stored under this location.");

		return ret;
	}
}
