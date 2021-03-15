package de.webis.copycat_spark.util.arc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.httpclient.Header;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.archive.archivespark.sparkling.http.HttpMessage;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.copycat_spark.app.ArgumentParsingUtil;
import de.webis.copycat_spark.util.arc.ArcInputFormat.MyARCReaderFactory;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Option;
import scala.collection.Map;

public class ARCParsingUtil {

	public static JavaPairRDD<LongWritable, ARCRecord> records(JavaSparkContext jsc, Namespace parsedArgs) {
		return records(jsc, parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
	}
	
	public static JavaPairRDD<LongWritable, ARCRecord> records(JavaSparkContext jsc, String input) {
		return jsc.newAPIHadoopFile(input, ArcInputFormat.class, LongWritable.class, ARCRecord.class, jsc.hadoopConfiguration());
	}

	public static String extractURL(ARCRecord record) {
		if(record == null) {
			return "NOT-AVAILABLE";
		}
		
		 Header[] headers = record.getHttpHeaders();
		 if(headers == null) {
			 return "NOT-AVAILABLE"; 
		 }
		 
		 for(Header header: headers) {
			 if("x_commoncrawl_OriginalURL".equalsIgnoreCase(header.getName())) {
				 return header.getValue();
			 }
		 }
		 
		 return "ORIGINAL_URL_NOT_AVAILABLE";
	}

	public static List<String> extractAllURLs(String file, InputStream is) {
		Iterator<ArchiveRecord> reader = MyARCReaderFactory.getIteratorOrEmptyIterator(new org.apache.hadoop.fs.Path(file), is);
		List<String> ret = new ArrayList<>();
		
		reader.forEachRemaining(i -> ret.add(extractURL((ARCRecord) i)));
		
		return ret;
	}
	
	@SneakyThrows
	public static void main(String[] args) {
		String file = "src/test/resources/data/cc-arc-sample/small-sample-1213893279526_0.arc";
		InputStream is = new FileInputStream(new File(file));
		Iterator<ArchiveRecord> reader = MyARCReaderFactory.getIteratorOrEmptyIterator(new org.apache.hadoop.fs.Path(file), is);
		
		reader.forEachRemaining(i -> System.out.println(reportSomeParsingMetaData(i)));
	}
	
	@SneakyThrows
	public static String reportSomeParsingMetaData(ArchiveRecord record) {
		HttpMessage message = httpMessageOrNull(record);
		java.util.Map<String, Object> ret = new LinkedHashMap<>();
		
		ret.put("url", extractURL((ARCRecord) record));
		ret.put("isHtml", isHtml(message));
		
		if (isHtml(message)) {
			ret.put("headerElements", message.headers().size());
			ret.put("contentEncoding", (message.contentEncoding().isEmpty() ? null : message.contentEncoding().get()));
			ret.put("bodyLength", failsaveBodyString(message).length());
		}
		
		return new ObjectMapper().writeValueAsString(ret);
	}
	
	private static String failsaveBodyString(HttpMessage message) {
		try {
			return message.bodyString();
		} catch (Exception e) {
			return "";
		}
	}
	
	private static boolean isHtml(HttpMessage message) {
		if(message == null || message.headers() == null || message.lowerCaseHeaders() == null) {
			return false;
		}
		Map<String, String> headers = message.lowerCaseHeaders();
		
		return !headers.get("content-type").isEmpty() && headers.get("content-type").get().contains("text/html");
	}
	
	@SneakyThrows
	private static HttpMessage httpMessageOrNull(ArchiveRecord record) {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		record.dump(os);
		InputStream is = new ByteArrayInputStream(os.toByteArray());
		Option<HttpMessage> ret = HttpMessage.get(is);
		
		return ret.isEmpty() ? null : ret.get();
	}
}
