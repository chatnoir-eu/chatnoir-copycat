package de.webis.cikm20_duplicates.app;

import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class CreateDocumentRepresentations {
	
	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);
		
		if(parsedArgs == null) {
			return;
		}
		
		try(JavaSparkContext context = context()) {
			JavaPairRDD<LongWritable, WarcRecord> records = WARCParsingUtil.records(context, parsedArgs);
			JavaRDD<String> tmp = records.map(i -> bla(i._2()));
			tmp.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
			
			//[Content-Length, Content-Type, WARC-Block-Digest, WARC-Concurrent-To, WARC-Date, WARC-IP-Address, WARC-Payload-Digest, WARC-Record-ID, WARC-Target-URI, WARC-Type, WARC-Warcinfo-ID]
		}
	}
	
	public static String bla(WarcRecord record) {
		Map<String, String> header = record.getHeader().getHeaderMetadata();
		String id = header.get("WARC-TREC-ID");
		if(id == null || id.isEmpty()) {
			id = header.get("WARC-Record-ID");
		}
		
		return 	"{\"uri\":\"" + header.get("WARC-Target-URI") +
				   "\",\"id\":\"" + id +
				   "\",\"contentLength\":\"" + header.get("Content-Length") +
				   "\",\"date\":\"" + header.get("WARC-Date") + "\"}";
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
			.addHelp(Boolean.TRUE)
			.build();
		
		ret.addArgument("-i", "--" + ArgumentParsingUtil.ARG_INPUT)
			.required(Boolean.TRUE)
			.help("The input path that is passed to JavaSparkContext.hadoopFile to extract Documents from warc files. E.g. 's3a://corpus-clueweb09'.");
		
		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT)
			.required(Boolean.TRUE)
			.help("The resulting document representations are stored under this location.");

		ret.addArgument("-f", "--" + ArgumentParsingUtil.ARG_FORMAT)
			.required(Boolean.TRUE)
			.choices(ArgumentParsingUtil.InputFormats.allInputFormats());
		
		return ret;
	}
}
