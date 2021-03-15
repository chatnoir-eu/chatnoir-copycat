package de.webis.copycat_spark.app;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Repartition {
	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
			
			input.repartition(parsedArgs.getInt(ArgumentParsingUtil.ARG_PARTITIONS))
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": Repartition");

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
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": Repartition")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
				"The input path that is repartitioned.");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The resulting repartitioned data is stored under this location.");

		ret.addArgument("-p", "--" + ArgumentParsingUtil.ARG_PARTITIONS).required(Boolean.TRUE)
				.type(Integer.class);

		return ret;
	}
}
