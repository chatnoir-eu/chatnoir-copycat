package de.webis.copycat_spark.app;

import java.net.URL;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import de.webis.copycat.DocumentPair;
import de.webis.copycat.Similarities;
import de.webis.copycat_spark.app.DeduplicateTrecRunFile.DefaultSimilarityCalculation;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.SneakyThrows;

public class CanonicalUrlPseudoSimilarityCalculation {
	
	@Test
	public void testWithNullAsInput() {
		boolean expected = false;
		boolean actual = similarAccordingToCanonicalUrls(null, null);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithNullFieldsAsInput() {
		boolean expected = false;
		CollectionDocument a = doc(null, null);
		CollectionDocument b = doc(null, null);
		boolean actual = similarAccordingToCanonicalUrls(a, b);
		
		Assert.assertEquals(expected, actual);
	}
	

	@Test
	public void testWithDifferentUrls() {
		boolean expected = false;
		CollectionDocument a = doc("https://www.a.de", null);
		CollectionDocument b = doc("https://www.b.de", null);
		boolean actual = similarAccordingToCanonicalUrls(a, b);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithMatchingCanonicalUrls() {
		boolean expected = true;
		CollectionDocument a = doc("https://www.a.de", null);
		CollectionDocument b = doc("https://www.b.de", "https://www.a.de");
		boolean actual = similarAccordingToCanonicalUrls(a, b);
		
		Assert.assertEquals(expected, actual);
	}
	
	
	@Test
	public void testWithMatchingCanonicalUrls2() {
		boolean expected = true;
		CollectionDocument a = doc("https://www.a.de", "https://www.c.de");
		CollectionDocument b = doc("https://www.b.de", "https://www.c.de");
		boolean actual = similarAccordingToCanonicalUrls(a, b);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithMatchingUrlDuplicate() {
		boolean expected = true;
		CollectionDocument a = doc("https://www.a.de", "https://www.c.de");
		CollectionDocument b = doc("https://www.a.de", "https://www.d.de");
		boolean actual = similarAccordingToCanonicalUrls(a, b);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithMatchingUrlDuplicate2() {
		boolean expected = true;
		CollectionDocument a = doc("https://www.a.de", "https://www.b.de");
		CollectionDocument b = doc("https://www.b.de", "https://www.d.de");
		boolean actual = similarAccordingToCanonicalUrls(a, b);
		
		Assert.assertEquals(expected, actual);
	}
	
	private CollectionDocument doc(String url, String canonicalUrl ) {
		CollectionDocument ret = Mockito.mock(CollectionDocument.class);
		Mockito.when(ret.getCanonicalUrl()).thenReturn(url(canonicalUrl));
		Mockito.when(ret.getUrl()).thenReturn(url(url));
	
		return ret;
	}
	
	@SneakyThrows
	private URL url(String url) {
		return url == null ? null : new URL(url);
	}
	
	private boolean similarAccordingToCanonicalUrls(CollectionDocument a, CollectionDocument b) {
		DefaultSimilarityCalculation sim = new DefaultSimilarityCalculation(Arrays.asList("url"));
		DocumentPair i = new DocumentPair(null, a, b, null, null);
		
		return 0.999 <= sim.calculateSimilarities(i).get("url");
	}
}
