package de.webis.cikm20_duplicates.app;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.chatnoir2.mapfile_generator.inputformats.WarcInputFormat;
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
			Class<? extends WarcInputFormat> inputFormat = ArgumentParsingUtil.InputFormats.valueOf(parsedArgs.get(ArgumentParsingUtil.ARG_FORMAT)).getInputFormat();
			
			JavaPairRDD<LongWritable, WarcRecord> records = context.newAPIHadoopFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT), inputFormat, LongWritable.class, WarcRecord.class, context.hadoopConfiguration());
			
			JavaRDD<String> tmp = records.map(i -> 
				"{\"uri\":\"" + i._2().getHeader().getHeaderMetadata().get("WARC-Target-URI") +
				   "\",\"id\":\"" + i._2().getHeader().getHeaderMetadata().get("WARC-TREC-ID") +
				   "\",\"contentLength\":\"" + i._2().getHeader().getHeaderMetadata().get("Content-Length") +
				   "\",\"date\":\"" + i._2().getHeader().getHeaderMetadata().get("WARC-Date") +"\"}"
				   
			);
			tmp.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": CreateDocumentRepresentations");

		return new JavaSparkContext(conf);
	}
	
	private static Namespace validArgumentsOrNull(String[] args) {
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
