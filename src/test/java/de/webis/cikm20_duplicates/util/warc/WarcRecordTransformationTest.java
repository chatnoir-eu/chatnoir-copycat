package de.webis.cikm20_duplicates.util.warc;

import java.net.URL;
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
import de.webis.cikm20_duplicates.app.CreateDocumentRepresentations.DocumentToTextTransformation;
import de.webis.cikm20_duplicates.spark.SparkIntegrationTestBase;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil.EsDocumentResolver;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil.HdfsMapFileDocumentResolver;
import lombok.SneakyThrows;

public class WarcRecordTransformationTest extends SparkIntegrationTestBase {
	@Test
	public void testWithNullInput() {
		Assert.assertNull(CreateDocumentRepresentations.transformToCollectionDocument(null, null));
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
	
	@Test
	public void approveTransformationOfCommonCrawlRecordWithMainContentExtraction() {
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
		
		CollectionDocument actual = transformToCollectionDocument(record, DocumentToTextTransformation.MAIN_CONTENT_EXTRACTION);
		
		Approvals.verifyAsJson(actual);
	}

	@Test
	@SneakyThrows
	public void approveTransformationOfExistingCommonCrawlRecord() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-Record-ID", "my-id-2");
		headers.put("WARC-Target-URI", "http://example.com");
		headers.put("WARC-Date", "01.01.1970");
		String documentContent = CollectionDocumentUtil.loadRawDocumentFromChatnoir(new URL("https://chatnoir.eu/cache?uuid=92105ce9-2938-5b89-a191-7a82fe5d8816&index=cc1704&raw"), CollectionDocumentUtil.RETRY_FINAL);
		WarcRecord record = record(headers, documentContent, "REsponse");
		
		CollectionDocument actual = transformToCollectionDocument(record, DocumentToTextTransformation.DEFAULT);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	@SneakyThrows
	public void approveTransformationOfExistingCommonCrawlRecord2REMOVE() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-Record-ID", "my-id-2");
		headers.put("WARC-Target-URI", "http://example.com");
		headers.put("WARC-Date", "01.01.1970");
		String documentContent = CollectionDocumentUtil.loadRawDocumentFromChatnoir(new URL("https://chatnoir.eu/cache?uuid=6336be0b-8971-50a2-9c8d-7a1f19cd1b66&index=cw09&raw"), CollectionDocumentUtil.RETRY_FINAL);
		WarcRecord record = record(headers, documentContent, "REsponse");
		
		CollectionDocument actual = transformToCollectionDocument(record, DocumentToTextTransformation.MAIN_CONTENT_EXTRACTION);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	@SneakyThrows
	public void approveTransformationOfExistinClueWebDocumentWithMainContentFromChatNoir() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-Record-ID", "my-id-2");
		headers.put("WARC-Target-URI", "http://example.com");
		headers.put("WARC-Date", "01.01.1970");
		String documentContent = new EsDocumentResolver().loadCollectionDocument("clueweb09-en0000-00-00009").getContent();
		WarcRecord record = record(headers, documentContent, "REsponse");
		
		CollectionDocument actual = transformToCollectionDocument(record, DocumentToTextTransformation.MAIN_CONTENT_EXTRACTION);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	@SneakyThrows
	public void approveTransformationOfNonExistinClueWebDocumentWithMainContentFromChatNoir() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-Record-ID", "my-id-2");
		headers.put("WARC-Target-URI", "http://example.com");
		headers.put("WARC-Date", "01.01.1970");
		CollectionDocument doc = new EsDocumentResolver().loadCollectionDocument("clueweb09-NON-EXISTING-en0000-00-00009");
		
		Assert.assertNull(doc);
	}

	@Test
	@SneakyThrows
	public void approveTransformationOfExistinClueWebDocumentFromHDFS() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-Record-ID", "my-id-2");
		headers.put("WARC-Target-URI", "http://example.com");
		headers.put("WARC-Date", "01.01.1970");
		CollectionDocument doc = new HdfsMapFileDocumentResolver("cw09", "clueweb09").loadCollectionDocument("clueweb09-en0000-00-00009");
		WarcRecord record = record(headers, doc.getContent(), "REsponse");
		
		CollectionDocument actual = transformToCollectionDocument(record, DocumentToTextTransformation.DEFAULT);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	@SneakyThrows
	public void approveTransformationOfExistinClueWebDocumentFromHDFSWithCanonicalUrl() {
		CollectionDocument doc = HdfsMapFileDocumentResolver.smartDocumentResolver().loadCollectionDocument("<urn:uuid:da8b10cf-af5d-43c8-8cf1-ae2f1fba994e>");
		String expectedUrl = "http://en.wikipedia.org/wiki/Canis_lupus_familiaris";
		String expectedCanonicalUrl = "http://en.wikipedia.org/wiki/Dog";
		
		Assert.assertEquals(expectedUrl, doc.getUrl().toString());
		Assert.assertEquals(expectedCanonicalUrl, doc.getCanonicalUrl().toString());
	}
	
	@Test
	@SneakyThrows
	public void approveTransformationOfNonExistinClueWebDocumentClueWebDocumentFromHDFS() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-Record-ID", "my-id-2");
		headers.put("WARC-Target-URI", "http://example.com");
		headers.put("WARC-Date", "01.01.1970");
		CollectionDocument doc = new HdfsMapFileDocumentResolver("cw09", "clueweb09").loadCollectionDocument("clueweb09-NON-EXISTING-en0000-00-00009");
		
		Assert.assertNull(doc);
	}
	
	@Test
	@SneakyThrows
	public void approveTransformationOfExistingCommonCrawlRecordWithMainContentExtraction() {
		Map<String, String> headers = new HashMap<>();
		headers.put("WARC-Record-ID", "my-id-2");
		headers.put("WARC-Target-URI", "http://example.com");
		headers.put("WARC-Date", "01.01.1970");
		String documentContent = CollectionDocumentUtil.loadRawDocumentFromChatnoir(new URL("https://chatnoir.eu/cache?uuid=92105ce9-2938-5b89-a191-7a82fe5d8816&index=cc1704&raw"), CollectionDocumentUtil.RETRY_FINAL);
		WarcRecord record = record(headers, documentContent, "REsponse");
		
		CollectionDocument actual = transformToCollectionDocument(record, DocumentToTextTransformation.MAIN_CONTENT_EXTRACTION);
		
		Approvals.verifyAsJson(actual);
	}
	
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
		return transformToCollectionDocument(record, null);
	}
	
	private CollectionDocument transformToCollectionDocument(WarcRecord record, DocumentToTextTransformation transformation) {
		return CreateDocumentRepresentations.transformToCollectionDocument(record, transformation);
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
