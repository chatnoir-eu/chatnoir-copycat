package de.webis.cikm20_duplicates.app;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.chatnoir2.mapfile_generator.warc.WarcHeader;
import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import de.webis.cikm20_duplicates.util.warc.WARCParsingUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

public class CreateWebGraph {
	public static void main(String[] args) {
		Namespace parsedArgs = validArgumentsOrNull(args);
		
		if(parsedArgs == null) {
			return;
		}
		
		try(JavaSparkContext context = context()) {
			JavaPairRDD<LongWritable, WarcRecord> records = WARCParsingUtil.records(context, parsedArgs);
			
			extractWebGraph(records)
				.saveAsTextFile(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT), BZip2Codec.class);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": CreateWebGraph");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> extractWebGraph(JavaPairRDD<LongWritable, WarcRecord> records) {
		JavaRDD<WebGraphNode> graphLinks = records
				.map(record -> extractWebGraphLinks(record))
				.filter(i -> i!= null);
		
		return graphLinks.filter(i -> i != null).map(i -> i.toString());
	}

	public static WebGraphNode extractWebGraph(WarcRecord record) {
		if(!CreateDocumentRepresentations.isWarcResponse(record)) {
			return null;
		}
		
		WarcHeader warcHeader = record.getHeader();
	
		String sourceURL = warcHeader.getHeaderMetadataItem("WARC-Target-URI");
		String crawlingTimestamp = warcHeader.getHeaderMetadataItem("WARC-Date");
		
		if(sourceURL == null || crawlingTimestamp == null) {
			return null;
		}
		
		return new WebGraphNode(sourceURL, crawlingTimestamp, anchors(record, sourceURL));
	}
	
	private static WebGraphNode extractWebGraphLinks(Tuple2<LongWritable, WarcRecord> record) {
		return extractWebGraph(record._2());
	}
	
	private static List<WebGraphAnchor> anchors(WarcRecord record, String sourceURL) {
		try {
			Document parsedDocument = Jsoup.parse(record.getContent(), sourceURL);
			
			return parsedDocument.select("a[href]").stream()
				.map(i -> WebGraphAnchor.fromElement(i))
				.collect(Collectors.toList());
		} catch(Exception e) {
			return Collections.emptyList();
		}
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class WebGraphNode implements Serializable {
		private String sourceURL, crawlingTimestamp;
		private List<WebGraphAnchor> anchors;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}

		@SneakyThrows
		public static WebGraphNode fromString(String src) {
			return new ObjectMapper().readValue(src, WebGraphNode.class);
		}
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class WebGraphAnchor implements Serializable {
		private String targetURL, anchorText;
		
		public static WebGraphAnchor fromElement(Element element) {
			return new WebGraphAnchor(element.attr("abs:href"), element.text());
		}
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
		ArgumentParser ret = ArgumentParsers.newFor(ArgumentParsingUtil.TOOL_NAME + ": CreateWebGraph")
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
			.choices(ArgumentParsingUtil.ALL_INPUT_FORMATS);
		
		return ret;
	}
}
