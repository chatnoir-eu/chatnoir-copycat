package de.webis.cikm20_duplicates.app;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.chatnoir2.mapfile_generator.warc.WarcHeader;
import de.webis.chatnoir2.mapfile_generator.warc.WarcHeader.WarcVersion;
import de.webis.cikm20_duplicates.spark.SparkIntegrationTestBase;
import lombok.SneakyThrows;
import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import scala.Tuple2;

public class CreateWebGraphTest extends SparkIntegrationTestBase {
	@Test
	public void approveParsingOfEmptyExample() {
		JavaPairRDD<LongWritable, WarcRecord> records = records();
		List<String> expected = Collections.emptyList();
		List<String> actual = webGraph(records);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	@SneakyThrows
	public void approveParsingOfSinglePageWithoutLinks() {
		Object actual = CreateWebGraph.extractWebGraph(r("", new URL("http://www.example.com"), "my-date-1"));
		
		Approvals.verify(actual.toString());
	}
	
	@Test
	@SneakyThrows
	public void approveParsingOfSinglePageWithSomeLinks() {
		Object actual = CreateWebGraph.extractWebGraph(r(
			"<a href=\"http://google.de/bla\">test-1</a> and <a href=\"bla\">test-2</a>.",
			new URL("http://www.my-example.com"),
			"my-date-2")
		);
		
		Approvals.verify(actual.toString());
	}

	private static List<String> webGraph(JavaPairRDD<LongWritable, WarcRecord> records) {
		JavaRDD<String> webGraph = CreateWebGraph.extractWebGraph(records);
		
		List<String> ret = webGraph.collect();
		Collections.sort(ret);
		
		return ret;
	}
	
	private static WarcRecord r(String content, URL targetUri, String date) {
		WarcHeader header = new WarcHeader(WarcVersion.WARC018);

		header.addHeaderMetadata("WARC-Target-URI", targetUri.toString());
		header.addHeaderMetadata("WARC-Date", date);
		
		return new WarcRecord(header) {
			@Override
			public String getContent() {
				return content;
			}
			
			@Override
			public String getRecordType() {
				return "response";
			}
		};
	}
	
	private JavaPairRDD<LongWritable, WarcRecord> records(WarcRecord...records) {
		return jsc().parallelize(Arrays.asList(records)).mapToPair(i -> new Tuple2<LongWritable, WarcRecord>(new LongWritable(0l), i));
	}
}
