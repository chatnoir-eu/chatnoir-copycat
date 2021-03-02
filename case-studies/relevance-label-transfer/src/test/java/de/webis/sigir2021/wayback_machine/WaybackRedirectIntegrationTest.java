package de.webis.sigir2021.wayback_machine;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.wayback_machine.JudgedDocumentsWarcReader;
import lombok.SneakyThrows;

public class WaybackRedirectIntegrationTest {
	@Test
	public void approveRedirectForCnnVideo() {
		String cdxRecord = "com,cnn)/video 20121221172709 http://www.cnn.com/video/ text/html 302 POPLDWWERZ2PUX2BRPCFNS2BB6ELQHMY 340";
		String expected = "http://web.archive.org/web/20121221172709/http://edition.cnn.com/video/";
		String actual = JudgedDocumentsWarcReader.resolveRedirect(cdxRecord);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveRedirectForTimePage() {
		String cdxRecord = "com,time)/time/photogallery/0,29307,1834628_1754174,00.html 20131121034724 http://www.time.com/time/photogallery/0,29307,1834628_1754174,00.html text/html 301 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ 541";
		String expected = "http://web.archive.org/web/20131121034724/http://content.time.com/time/photogallery/0,29307,1834628_1754174,00.html";
		String actual = JudgedDocumentsWarcReader.resolveRedirect(cdxRecord);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkRecursiveRedirects() {
		String cdxRecord = "com,mahalo)/marian_robinson 20120113224330 http://www.mahalo.com/Marian_Robinson/ text/html 301 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ 257";
		String expected = "http://web.archive.org/web/20120113224331/http://www.mahalo.com/marian-robinson/";
		String actual = JudgedDocumentsWarcReader.resolveRedirect(cdxRecord);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	@SneakyThrows
	public void testParsingOfCnnVideoRedirectLink() {
		String html = IOUtils.toString(WaybackRedirectIntegrationTest.class.getResourceAsStream("/wayback-redirect-cnn-video.html"));
		String expected = "http://web.archive.org/web/20121221172709/http://edition.cnn.com/video/";
		String actual = JudgedDocumentsWarcReader.extractRedirectMessageFromHtml(html);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	@SneakyThrows
	public void testParsingOfForTimePage() {
		String html = IOUtils.toString(WaybackRedirectIntegrationTest.class.getResourceAsStream("/wayback-redirect-time.html"));
		String expected = "http://web.archive.org/web/20131121034724/http://content.time.com/time/photogallery/0,29307,1834628_1754174,00.html";
		String actual = JudgedDocumentsWarcReader.extractRedirectMessageFromHtml(html);

		Assert.assertEquals(expected, actual);
	}
}
