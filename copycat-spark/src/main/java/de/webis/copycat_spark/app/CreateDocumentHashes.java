package de.webis.copycat_spark.app;

import static de.webis.copycat_spark.app.CreateDocumentRepresentations.transformToCollectionDocument;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import de.webis.copycat_spark.app.CreateDocumentRepresentations.DocumentToTextTransformation;
import de.webis.copycat_spark.util.warc.WARCParsingUtil;
import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class CreateDocumentHashes {

	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaPairRDD<LongWritable, WarcRecord> records = WARCParsingUtil.records(context, parsedArgs);
			JavaRDD<CollectionDocument> parsedDocuments = records.map(i -> transformToCollectionDocument(i._2(), DocumentToTextTransformation.DEFAULT))
					.filter(i -> i != null);
			
			if (parsedDocuments.getNumPartitions() < 100) {
				parsedDocuments = parsedDocuments.repartition(parsedDocuments.getNumPartitions()*100);
			}

			parsedDocuments.map(i -> new DocumentHash(i))
				.filter(i -> i != null)
				.map(i -> i.toString())
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}
	
	private static ArgumentParser argParser() {
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": CreateDocumentHashes")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
				"The input path that is passed to JavaSparkContext.hadoopFile to extract Documents from warc files. E.g. 's3a://corpus-clueweb09'.");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The resulting document hashes are stored under this location.");

		ret.addArgument("-f", "--" + ArgumentParsingUtil.ARG_FORMAT).required(Boolean.TRUE)
				.choices(ArgumentParsingUtil.ALL_INPUT_FORMATS);

		return ret;
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": CreateDocumentHashes");

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
}
