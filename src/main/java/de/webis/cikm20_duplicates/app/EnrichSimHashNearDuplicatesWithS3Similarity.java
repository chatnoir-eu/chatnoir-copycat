package de.webis.cikm20_duplicates.app;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterators;

import de.webis.cikm20_duplicates.spark.SparkEnrichRelevanceTransferPairs;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil.DocumentResolver;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

public class EnrichSimHashNearDuplicatesWithS3Similarity {
	
	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaRDD<String> inputCsv = context.textFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
			
			enrichNearDuplicatesWithS3Score(inputCsv, docResolver(parsedArgs))
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}
	
	public static JavaRDD<String> enrichNearDuplicatesWithS3Score(JavaRDD<String> simHashNearDuplicates, DocumentResolverFactory docResolverFactory) {
		return simHashNearDuplicates
				.mapToPair(i -> new Tuple2<>(firstId(i), i))
				.groupByKey()
				.flatMap(i -> enrichS3Score(i, docResolverFactory));
	}
	
	private static Iterator<String> enrichS3Score(Tuple2<String, Iterable<String>> groupForFirstId, DocumentResolverFactory docResolverFactory) {
		DocumentResolver docResolver = docResolverFactory.get();
		String firstId = groupForFirstId._1();
		CollectionDocument firstDoc = docResolver.loadCollectionDocument(firstId);
		
		return Iterators.transform(
			groupForFirstId._2().iterator(),
			i -> addS3ScoreToCsvLine(firstDoc, i, docResolver)
		);
	}
	
	private static String addS3ScoreToCsvLine(CollectionDocument firstDoc, String csvLine, DocumentResolver docResolver) {
		String secondId = secondId(csvLine);
		CollectionDocument secondDoc = docResolver.loadCollectionDocument(secondId);
		
		if(firstDoc == null || secondDoc == null) {
			return csvLine +",-1";
		} else {
			double s3Score = SparkEnrichRelevanceTransferPairs.s3Score(firstDoc, secondDoc);
			return csvLine +"," + String.format( "%.4f", s3Score);
		}
	}
	
	private static String firstId(String csvLine) {
		return StringUtils.substringBefore(csvLine, ",");
	}
	
	private static String secondId(String csvLine) {
		return StringUtils.substringBefore(StringUtils.substringAfter(csvLine +",", ","), ",");
	}
	
	static DocumentResolverFactory docResolver(Namespace args) {
		Map<String, Object> argsAttributes = new LinkedHashMap<>(args.getAttrs());
		
		return () -> CollectionDocumentUtil.HdfsMapFileDocumentResolver.fromArgs(argsAttributes);
	}
	
	public static interface DocumentResolverFactory extends Supplier<DocumentResolver>, Serializable {};
	
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
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": EnrichSimHashNearDuplicatesWithS3Similarity")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
				"Near-duplicate file in csv format");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The resulting csv enriched with an additional s3Score column is stored under this location.");
		
		ret.addArgument("--" + ArgumentParsingUtil.UUID_PREFIX).required(Boolean.TRUE)
				.type(String.class)
				.help("The uuid prefix that is used to retrieve the document from chatnoirs mapfiles.");
		
		ret.addArgument("--" + ArgumentParsingUtil.UUID_INDEX).required(Boolean.TRUE)
				.type(String.class)
				.help("The uuid index that is used to retrieve the document from chatnoirs mapfiles.");

		return ret;
	}
}
