package de.webis.cikm20_duplicates.app;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import de.webis.cikm20_duplicates.spark.SparkCanonicalLinkGraphExtraction;
import de.webis.cikm20_duplicates.spark.SparkCreateSourceDocuments;
import de.webis.cikm20_duplicates.util.SourceDocuments.DocumentWithFingerprint;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

public class CreateDocumentRepresentations {

	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaPairRDD<LongWritable, WarcRecord> records = WARCParsingUtil.records(context, parsedArgs);
			JavaRDD<CollectionDocument> parsedDocuments = parsedDocuments(records);
			if (parsedDocuments.getNumPartitions() < 100) {
				parsedDocuments = parsedDocuments.repartition(1000);
			}

			JavaRDD<DocumentWithFingerprint> fingerprints = SparkCreateSourceDocuments.fingerprintAllDocuments(null,
					parsedDocuments, SparkCreateSourceDocuments.PRODUCTION_FINGERPRINTS);

			fingerprints.filter(i -> i != null).map(i -> i.toString())
					.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}

	private static JavaRDD<CollectionDocument> parsedDocuments(JavaPairRDD<LongWritable, WarcRecord> records) {
		return records.map(i -> {
			Tuple2<Map<String, String>, String> r = transformToCollectionDocument(i._2());
			return r == null ? null : transformToCollectionDocument(r._1(), r._2());
		}).filter(i -> i != null);
	}

	@SneakyThrows
	public static Tuple2<Map<String, String>, String> transformToCollectionDocument(WarcRecord record) {
		if (record == null) {
			return null;
		}

		Map<String, String> header = lowercasedHeaders(record);
		String contentBody = record.getContent();

		if (contentBody.getBytes().length > 1024 * 1024) {
			// ignore large files
			return null;
		}

		return new Tuple2<>(header, contentBody);
	}

	public static CollectionDocument transformToCollectionDocument(Map<String, String> header, String contentBody) {
		try {
			return transformToCollectionDocument(header, Jsoup.parse(contentBody));
		} catch (Exception e) {
			return null;
		}
	}

	@SneakyThrows
	private static CollectionDocument transformToCollectionDocument(Map<String, String> header, Document doc) {
		String id = header.get("warc-trec-id");
		if (id == null || id.isEmpty()) {
			id = header.get("warc-record-id");
		}

		String targetUri = header.get("warc-target-uri");

		CollectionDocument ret = CollectionDocument.collectionDocument(doc.text(), id);
		ret.setUrl(new URL(targetUri));
		ret.setCanonicalUrl(SparkCanonicalLinkGraphExtraction.extractCanonicalLinkOrNull(targetUri, doc));
		ret.setCrawlingTimestamp(header.get("warc-date"));

		return ret;
	}

	private static Map<String, String> lowercasedHeaders(WarcRecord record) {
		return new HashMap<>(record.getHeader().getHeaderMetadata().entrySet().stream()
				.collect(Collectors.toMap(i -> i.getKey().trim().toLowerCase(), i -> i.getValue())));
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": CreateDocumentRepresentations");

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
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": CreateDocumentRepresentations")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
				"The input path that is passed to JavaSparkContext.hadoopFile to extract Documents from warc files. E.g. 's3a://corpus-clueweb09'.");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The resulting document representations are stored under this location.");

		ret.addArgument("-f", "--" + ArgumentParsingUtil.ARG_FORMAT).required(Boolean.TRUE)
				.choices(ArgumentParsingUtil.InputFormats.allInputFormats());

		return ret;
	}
}
