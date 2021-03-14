package de.webis.cikm20_duplicates.app.url_groups;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.cikm20_duplicates.app.ArgumentParsingUtil;
import de.webis.cikm20_duplicates.util.SourceDocuments.DocumentWithFingerprint;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

/**
 * This spark job repartitions all documents so that documents with the same URL or same canonical URL belong to the same partition.
 * 
 * 
 * @author Maik Fröbe
 *
 */
public class UrlBasedRepartitioningOfDocuments {

	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
			
			repartitionDocumentsWithTheSameUrlOrCanonicalUrlToTheSamePart(input, parsedArgs.getInt(ArgumentParsingUtil.ARG_PARTITIONS))
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}
	
	public static JavaRDD<String> repartitionDocumentsWithTheSameUrlOrCanonicalUrlToTheSamePart(JavaRDD<String> input, int numPartitions) {
		return repartitionDocumentsWithTheSameUrlOrCanonicalUrlToTheSamePart(input, new HashPartitioner(numPartitions));
	}
	
	public static JavaRDD<String> repartitionDocumentsWithTheSameUrlOrCanonicalUrlToTheSamePart(JavaRDD<String> input, Partitioner partitioner) {
		JavaRDD<DocumentWithFingerprint> docs = input.map(i -> DocumentWithFingerprint.fromString(i));
		JavaPairRDD<String, DocumentForUrlDeduplication> ret = docs.map(i -> DocumentForUrlDeduplication.fromDocumentWithFingerprint(i))
					.filter(i -> i != null)
					.mapToPair(i -> new Tuple2<>(i.getCanonicalURL().toString(), i));
		
		ret = ret.repartitionAndSortWithinPartitions(partitioner);
		
		return ret.map(i -> i._2().toString());
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": UrlBasedRepartitioningOfDocuments");

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
				"The input path that contains all document representations.");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The repartitioned representations are stored under this location.");

		ret.addArgument("--" + ArgumentParsingUtil.ARG_PARTITIONS).required(Boolean.FALSE)
				.type(Integer.class)
				.setDefault(10000)
				.help("The number of partitions.");

		return ret;
	}
}
