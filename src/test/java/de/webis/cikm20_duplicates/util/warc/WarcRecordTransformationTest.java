package de.webis.cikm20_duplicates.util.warc;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import de.webis.chatnoir2.mapfile_generator.warc.WarcHeader;
import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import de.webis.cikm20_duplicates.app.CreateDocumentRepresentations;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class WarcRecordTransformationTest {
	@Test
	public void testWithNullInput() {
		Assert.assertNull(CreateDocumentRepresentations.transformToCollectionDocument(null));
	}
	
	@Test
	public void approveTransformationOfClueWebRecord() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-TREC-ID", "my-id-1");
		headers.put("WARC-Target-URI", "http://example.com");
		headers.put("WARC-Date", "01.01.1970");
		WarcRecord record = record(headers, "my-main-content", "RESPONSE");
		
		CollectionDocument actual = transformToCollectionDocument(record);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveTransformationOfCommonCrawlRecord() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-Record-ID", "my-id-2");
		headers.put("WARC-Target-URI", "http://example.com");
		headers.put("WARC-Date", "01.01.1970");
		WarcRecord record = record(headers, "<!DOCTYPE html>\n" + 
				"<html lang=\"de\">\n" + 
				"  <head>\n" + 
				"    <meta charset=\"utf-8\">\n" + 
				"    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" + 
				"    <title>Titel</title>\n" + 
				"    <link rel=\"canonical\" href=\"https://example.com/test-123/\" />" +
				"  </head>\n" + 
				"  <body>Test 1 2 3\n" + 
				"\n" + 
				"  </body>\n" + 
				"</html>",
				"REsponse");
		
		CollectionDocument actual = transformToCollectionDocument(record);
		
		Approvals.verifyAsJson(actual);
	}
	
//	@Test
//	public void approveTransformationOfCommonCrawlRecordWithHttpHeader() {
//		Map<String, String> headers = new HashMap<>();
//		headers.put("WARC-Record-ID", "my-id-2");
//		headers.put("WARC-Target-URI", "http://example.com");
//		headers.put("WARC-Date", "01.01.1970");
//		
//		//FIXME: This does not happen, since the WARCRecord does already Parse the HTTP-Headers
//		WarcRecord record = record(headers, "HTTP/1.x 200 OK\n" +
//				"Transfer-Encoding: foo-bar\n" +
//				"Vary: Accept-Encoding, Cookie, User-Agent\n\n" +
//				"<!DOCTYPE html>\n" + 
//				"<html lang=\"de\">\n" + 
//				"  <head>\n" + 
//				"    <meta charset=\"utf-8\">\n" + 
//				"    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" + 
//				"    <title>Titel</title>\n" + 
//				"    <link rel=\"canonical\" href=\"https://example.com/test-123/\" />" +
//				"  </head>\n" + 
//				"  <body>Test 1 2 3\n" + 
//				"\n" + 
//				"  </body>\n" + 
//				"</html>",
//				"REsponse");
//		
//		CollectionDocument actual = transformToCollectionDocument(record);
//		
//		Approvals.verifyAsJson(actual);
//	}
	
	@Test
	public void approveTransformationOfClueWebRecordWithInvalidCase() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-Trec-ID", "my-id-1");
		headers.put("WARC-target-URI", "http://example.com");
		headers.put("WARC-date", "01.01.1970");
		WarcRecord record = record(headers, "my-main-content", "response");
		
		CollectionDocument actual = transformToCollectionDocument(record);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveTransformationOfClueWebRecordWithResponseTypeRequest() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-Trec-ID", "my-id-1");
		headers.put("WARC-target-URI", "http://example.com");
		headers.put("WARC-date", "01.01.1970");
		WarcRecord record = record(headers, "my-main-content", "request");
		
		CollectionDocument actual = transformToCollectionDocument(record);

		Assert.assertNull(actual);
	}
	
	@Test()
	public void approveTransformationOfClueWebRecordWithInvalidCaseAndInvalidURL() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-Trec-ID", "my-id-1");
		headers.put("WARC-target-URI", "example.com");
		headers.put("WARC-date", "01.01.1970");
		WarcRecord record = record(headers, "my-main-content", "resPonse");
		
		CollectionDocument actual = transformToCollectionDocument(record);
		
		Approvals.verifyAsJson(actual);
	}
	
	private CollectionDocument transformToCollectionDocument(WarcRecord record) {
		return CreateDocumentRepresentations.transformToCollectionDocument(record);
	}

	private static WarcRecord record(Map<String, String> headers, String body, String recordType) {
		WarcHeader header = Mockito.mock(WarcHeader.class);
		Mockito.when(header.getHeaderMetadata()).thenReturn(new TreeMap<>(headers));
		
		WarcRecord ret = Mockito.mock(WarcRecord.class);
		Mockito.when(ret.getContent()).thenReturn(body);
		Mockito.when(ret.getHeader()).thenReturn(header);
		Mockito.when(ret.getRecordType()).thenReturn(recordType);
		
		return ret;
	}
}