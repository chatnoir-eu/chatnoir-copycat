package de.webis.cikm20_duplicates.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;

public class ReportMimeTypesTest extends SharedJavaSparkContext {
	@Test
	public void testWarcRecordWithMissingContentType() {
		WarcRecord record = r("RESPONSE", "CONTENTss-TYPE", null);
		
		String expected = "{\"type\":\"response\",\"contentType\":\"null\"}";
		String actual = ReportMimeTypes.extractMimeType(record);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWarcRecordWithMissingContentTypeAndRequest() {
		WarcRecord record = r("ReQuest", "CONTENT-TYPE", null);
		
		String expected = "{\"type\":\"request\",\"contentType\":\"null\"}";
		String actual = ReportMimeTypes.extractMimeType(record);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWarcRecordWithCorrectMimeTypeWithEncoding() {
		WarcRecord record = r("RESPONSE", "Content-Type", "text/html; charset=UTF-8");
		
		String expected = "{\"type\":\"response\",\"contentType\":\"text/html\"}";
		String actual = ReportMimeTypes.extractMimeType(record);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWarcRecordWithCorrectMimeTypeWithoutEncoding() {
		WarcRecord record = r("RESPONSE", "Content-Type", "text/html");
		
		String expected = "{\"type\":\"response\",\"contentType\":\"text/html\"}";
		String actual = ReportMimeTypes.extractMimeType(record);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWarcRecordWithIncorrectMimeTypeWithoutEncoding() {
		WarcRecord record = r("response", "content-type", "bla");
		
		String expected = "{\"type\":\"response\",\"contentType\":\"bla\"}";
		String actual = ReportMimeTypes.extractMimeType(record);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testGroupingOfMimeTypes() {
		JavaRDD<String> mimeTypes = jsc().parallelize(Arrays.asList(
				"{\"type\":\"response\",\"contentType\":\"text/html\"}",
				"{\"type\":\"response\",\"contentType\":\"bla\"}",
				"{\"type\":\"response\",\"contentType\":\"text/html\"}"
		));
		
		List<String> actual = ReportMimeTypes.groupMimeTypes(mimeTypes).collect();
		actual = new ArrayList<>(actual);
		Collections.sort(actual);
		
		Approvals.verifyAsJson(actual);
	}
	
	private static WarcRecord r(String type, String contentTypeName, String contentType) {
		Map<String, String> headers = new HashMap<>();
		headers.put(contentTypeName, contentType);

		WarcRecord ret = Mockito.mock(WarcRecord.class);
		Mockito.when(ret.getRecordType()).thenReturn(type);
		Mockito.when(ret.getContentHeaders()).thenReturn(new TreeMap<>(headers));
		
		return ret;
	}
}
