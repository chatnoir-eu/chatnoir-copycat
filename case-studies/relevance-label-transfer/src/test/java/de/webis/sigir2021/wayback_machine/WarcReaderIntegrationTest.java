package de.webis.sigir2021.wayback_machine;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.wayback_machine.JudgedDocumentsWarcReader;

public class WarcReaderIntegrationTest {
	@Test
	public void approveSnapshotsForCw19493() {
		String documentId = "clueweb09-en0000-02-19493";
		int topic = 1;
		
		String actual = new JudgedDocumentsWarcReader("src/test/resources/sample-input").entries(topic, documentId);
		Approvals.verify(actual);
	}
	
	@Test
	public void approveSnapshotsForCw02851() {
		String documentId = "clueweb09-enwp03-13-02851";
		int topic = 1;
		
		String actual = new JudgedDocumentsWarcReader("src/test/resources/sample-input").entries(topic, documentId);
		Approvals.verify(actual);
	}
	
	@Test
	public void testThatCorrectDocumentIsSelectedFromMultipleSnapshots() {
		String documentId = "clueweb09-enwp03-13-02851";
		int topic = 1;
		
		String actual = new JudgedDocumentsWarcReader("src/test/resources/sample-input").bestMatchingCW12SnapshotOrNull(topic, documentId);
		Assert.assertEquals("org,wikipedia,en)/wiki/ruth_ndesandjo 20120315144017 http://en.wikipedia.org:80/wiki/Ruth_Ndesandjo text/html 200 6GYMDNUDRFTIPSHSPIYIOPJAGODHSZXA 71883", actual);
	}
	
	@Test
	public void testThatNoRedirectRequestIsExtractedForDocumentWith200erResponse() {
		String documentId = "clueweb09-enwp03-13-02851";
		int topic = 1;
		
		String actual = new JudgedDocumentsWarcReader("src/test/resources/sample-input").bestMatchingCW12RedirectOrNull(topic, documentId);
		Assert.assertNull(actual);
	}
	
	@Test
	public void testThatCorrectRedirectRequestIsExtractedForDocumentWith301erResponse() {
		String documentId = "clueweb09-en0007-79-21751";
		String expected = "com,jamespot)/a/125441-barack-obama-s-family-tree.html 20120113225712 http://www.jamespot.com/a/125441-Barack-Obama-s-Family-Tree.html text/html 301 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ 484";
		int topic = 1;
		
		String actual = new JudgedDocumentsWarcReader("src/test/resources/sample-input").bestMatchingCW12RedirectOrNull(topic, documentId);
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testThatCorrectDocumentIsSelectedFromMultipleSnapshots2() {
		String documentId = "clueweb09-en0000-02-19493";
		int topic = 1;
			
		String actual = new JudgedDocumentsWarcReader("src/test/resources/sample-input").bestMatchingCW12SnapshotOrNull(topic, documentId);
		Assert.assertNull(actual);
	}
	
	@Test
	public void testParsingOfDate() {
		String expected = "2011/9/910-11:09:49";
		Date actual = JudgedDocumentsWarcReader.parseDate("com,cellphonenews)/category/usa_canada_phones/3g_cell_phones 20110910110949 http://www.cellphonenews.com/category/usa_canada_phones/3g_cell_phones text/html 200 E3OYLZFLBA3DHLJU23RQ3ZGHGDUTGGCK 12439");
		
		Assert.assertEquals(expected, sdf().format(actual));
	}
	
	@Test
	public void testParsingOfDate2() {
		String expected = "2009/1/127-05:16:46";
		Date actual = JudgedDocumentsWarcReader.parseDate("com,mahalo)/abo_obama 20090127051646 http://mahalo.com:80/Abo_Obama text/html 302 POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY 309");
		
		Assert.assertEquals(expected, sdf().format(actual));
	}
	
	private static SimpleDateFormat sdf() {
		SimpleDateFormat ret = new SimpleDateFormat("yyyy/M/Mdd-HH:mm:ss");
		ret.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
		
		return ret;
	}

	@Test
	public void testParsingOfResponseCode() {
		int expected = 200;
		int actual = JudgedDocumentsWarcReader.parseResponseCode("com,cellphonenews)/category/usa_canada_phones/3g_cell_phones 20110910110949 http://www.cellphonenews.com/category/usa_canada_phones/3g_cell_phones text/html 200 E3OYLZFLBA3DHLJU23RQ3ZGHGDUTGGCK 12439");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testParsingOfResponseCodeForNon302Code() {
		int expected = 302;
		int actual = JudgedDocumentsWarcReader.parseResponseCode("com,mahalo)/abo_obama 20090127051646 http://mahalo.com:80/Abo_Obama text/html 302 POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY 309");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testThatEntriesOutsideOfCw12CrawlingRangeAreRemoved() {
		List<String> entries = Arrays.asList(
			"com,mahalo)/abo_obama 20090127051646 http://mahalo.com:80/Abo_Obama text/html 200 POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY 309",
			"com,mahalo)/abo_obama 20130127051646 http://mahalo.com:80/Abo_Obama text/html 200 POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY 309"
		);
		
		List<String> remaining = JudgedDocumentsWarcReader.keepResponsesInCw12CrawlingTime(entries);
		
		Assert.assertTrue(remaining.isEmpty());
	}
	
	@Test
	public void testThatEntriesWithinCw12CrawlingRangeAreKept() {
		List<String> entries = Arrays.asList(
			"com,mahalo)/abo_obama 20111027051646 http://mahalo.com:80/Abo_Obama text/html 200 POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY 309",
			"com,mahalo)/abo_obama 20111227051646 http://mahalo.com:80/Abo_Obama text/html 200 POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY 309",
			"com,mahalo)/abo_obama 20120727051646 http://mahalo.com:80/Abo_Obama text/html 200 POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY 309",
			"com,mahalo)/abo_obama 20121227051646 http://mahalo.com:80/Abo_Obama text/html 200 POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY 309"
		);
		List<String> expected = Arrays.asList(
			"com,mahalo)/abo_obama 20111227051646 http://mahalo.com:80/Abo_Obama text/html 200 POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY 309",
			"com,mahalo)/abo_obama 20120727051646 http://mahalo.com:80/Abo_Obama text/html 200 POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY 309"
		);
		
		List<String> actual = JudgedDocumentsWarcReader.keepResponsesInCw12CrawlingTime(entries);
		
		Assert.assertEquals(expected, actual);
	}
}
