package de.webis.cikm20_duplicates.app;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

public class ReportMimeTypes {

	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);

		if (parsedArgs == null) {
			return;
		}

		try (JavaSparkContext context = context()) {
			JavaPairRDD<LongWritable, WarcRecord> records = WARCParsingUtil.records(context, parsedArgs);
			JavaRDD<String> parsedMimeTypes = records.map(i -> extractMimeType(i._2())).filter(i -> i != null);

			groupMimeTypes(parsedMimeTypes).saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT));
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": ReportMimeTypes");

		return new JavaSparkContext(conf);
	}
	
	public static String extractMimeType(WarcRecord record) {
		if(record == null|| record.getRecordType() == null || record.getContentHeaders() == null) {
			return null;
		}
		
		Map<String, String> ret = new LinkedHashMap<>();
		ret.put("type", record.getRecordType().toLowerCase().trim());
		ret.put("contentType", contentType(CreateDocumentRepresentations.lowercasedHeaders(record.getContentHeaders())));
		
		return serialize(ret);
	}

	
	private static String contentType(Map<String, String> contentHeaders) {
		if(contentHeaders == null || !contentHeaders.containsKey("content-type") || contentHeaders.get("content-type") == null) {
			return "null";
		} else {
			return StringUtils.substringBefore(contentHeaders.get("content-type").toLowerCase(), ";").trim();
		}
	}


	@SneakyThrows
	private static String serialize(Object o) {
		return new ObjectMapper().writeValueAsString(o);
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
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": ReportMimeTypes")
				.addHelp(Boolean.TRUE).build();

		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT).required(Boolean.TRUE).help(
				"The input path that is passed to JavaSparkContext.hadoopFile to extract Documents from warc files. E.g. 's3a://corpus-clueweb09'.");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT).required(Boolean.TRUE)
				.help("The resulting aggregations are stored under this location.");

		ret.addArgument("-f", "--" + ArgumentParsingUtil.ARG_FORMAT).required(Boolean.TRUE)
				.choices(ArgumentParsingUtil.InputFormats.allInputFormats());

		return ret;
	}

	public static JavaRDD<String> groupMimeTypes(JavaRDD<String> mimeTypes) {
		return mimeTypes
				.mapToPair(i -> new Tuple2<String, Long>(i, 1l)).reduceByKey((a,b) -> a+b)
				.map(i -> reportCount(i._1(), i._2()))
				.repartition(1);
	}

	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static String reportCount(String json, Long count) {
		Map<String, String> parsedJson = new ObjectMapper().readValue(json, Map.class);
		
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("type", parsedJson.get("type"));
		ret.put("contentType", parsedJson.get("contentType"));
		ret.put("count", count);

		return serialize(ret);
	}
}
