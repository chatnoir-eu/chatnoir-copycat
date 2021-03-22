package de.webis.copycat_spark.app;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.copycat_spark.util.SourceDocuments.DocumentWithFingerprint;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class CreationOfInclusionLists {

	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaRDD<String> allIds = context.textFile(parsedArgs.getString(ARG_ALL_IDS));
			JavaRDD<String> idsToExclude = context.textFile(parsedArgs.getString(ARG_EXCLUSION_IDS));
			
			createInclusionList(allIds, idsToExclude, parsedArgs)
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}
	
	private static final String ARG_ALL_IDS = "allIds",
								ARG_EXCLUSION_IDS = "exclusionIds",
								ARG_USE_DOCUMENT_REPRESENTATIONS = "docRepresentations";
	
	public static JavaRDD<String> createInclusionList(JavaRDD<String> allIds, JavaRDD<String> idsToExclude, Namespace parsedArgs) {
		if(parsedArgs.getBoolean(ARG_USE_DOCUMENT_REPRESENTATIONS)) {
			allIds = documentRepresentationsToDocumentIds(allIds, idsToExclude, parsedArgs.getInt(ArgumentParsingUtil.ARG_PARTITIONS));
		} 

		return createInclusionList(allIds, idsToExclude, parsedArgs.getInt(ArgumentParsingUtil.ARG_PARTITIONS));
	}
	
	private static JavaRDD<String> documentRepresentationsToDocumentIds(JavaRDD<String> documentRepresentations, JavaRDD<String> idsToExclude, int partitions) {
		return documentRepresentations.filter(i -> i != null)
				.map(i -> DocumentWithFingerprint.fromString(i))
				.filter(i -> i != null)
				.map(i -> i.getDocId())
				.filter(i -> i != null);
	}
	
	private static JavaRDD<String> createInclusionList(JavaRDD<String> allIds, JavaRDD<String> idsToExclude, int partitions) {
		return allIds.subtract(idsToExclude).repartition(partitions);
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": CreationOfInclusionLists");

		return new JavaSparkContext(conf);
	}
	
	private static ArgumentParser argParser() {
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": CreationOfInclusionLists")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("--" + ARG_ALL_IDS).required(Boolean.TRUE)
			.help("The ids containing all ids in the dataset.");
		
		ret.addArgument("--" + ARG_EXCLUSION_IDS).required(Boolean.TRUE)
			.help("The ids cthat must be excluded in the dataset.");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The resulting inclusion list is stored at this position.");

		ret.addArgument("--" + ArgumentParsingUtil.ARG_PARTITIONS)
			.type(Integer.class)
			.required(Boolean.FALSE)
			.setDefault(1);
		
		ret.addArgument("--" + ARG_USE_DOCUMENT_REPRESENTATIONS)
			.required(Boolean.FALSE)
			.type(Boolean.class)
			.setDefault(false)
			.help("The ids of this dataset are passed as one id per file (when this argument is false), or as document representations (when this argument is true).");

		return ret;
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
	
}
