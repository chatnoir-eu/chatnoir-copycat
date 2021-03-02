package de.webis.sigir2021.trec;

import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.trec.CanonicalDocuments;

public class CanonicalDocumentsIntegrationTest {
	@Test
	public void checkThatUnknownDocumentIsCanonicalByDefault() {
		String input = "non-existing-document";
		String expected = "non-existing-document";
		String actual = CanonicalDocuments.canonicalDocument(input);
		
		Assert.assertEquals(expected, actual);
		Assert.assertFalse(CanonicalDocuments.isDuplicate(input));
	}
	
	// {"hash":"1492","ids":["clueweb12-0402wb-69-05743","clueweb12-0108wb-13-04751"]}
	
	@Test
	public void checkThatCanonicalCW12DocumentIsCanonical() {
		String input = "clueweb12-0108wb-13-04751";
		String expected = "clueweb12-0108wb-13-04751";
		String actual = CanonicalDocuments.canonicalDocument(input);
		
		Assert.assertEquals(expected, actual);
		Assert.assertFalse(CanonicalDocuments.isDuplicate(input));
	}
	
	@Test
	public void checkCanonicalCW12DocumentForDuplicate() {
		String input = "clueweb12-0402wb-69-05743";
		String expected = "clueweb12-0108wb-13-04751";
		String actual = CanonicalDocuments.canonicalDocument(input);
		
		Assert.assertEquals(expected, actual);
		Assert.assertTrue(CanonicalDocuments.isDuplicate(input));
	}
	
	// {"hash":"1001","ids":["clueweb09-enwp03-37-22832","clueweb09-enwp01-47-05055","clueweb09-enwp03-35-22324","clueweb09-enwp03-23-22557","clueweb09-enwp03-20-22783","clueweb09-enwp03-29-22541"]}
	
	@Test
	public void checkThatCanonicalCW09DocumentIsCanonical() {
		String input = "clueweb09-enwp01-47-05055";
		String expected = "clueweb09-enwp01-47-05055";
		String actual = CanonicalDocuments.canonicalDocument(input);
		
		Assert.assertEquals(expected, actual);
		Assert.assertFalse(CanonicalDocuments.isDuplicate(input));
	}
	
	@Test
	public void checkCanonicalCW09DocumentForDuplicate() {
		String input = "clueweb09-enwp03-20-22783";
		String expected = "clueweb09-enwp01-47-05055";
		String actual = CanonicalDocuments.canonicalDocument(input);
		
		Assert.assertEquals(expected, actual);
		Assert.assertTrue(CanonicalDocuments.isDuplicate(input));
	}
	
	@Test
	public void checkCanonicalCW09DocumentForDuplicate2() {
		String input = "clueweb09-enwp03-29-22541";
		String expected = "clueweb09-enwp01-47-05055";
		String actual = CanonicalDocuments.canonicalDocument(input);
		
		Assert.assertEquals(expected, actual);
		Assert.assertTrue(CanonicalDocuments.isDuplicate(input));
	}
	
	@Test
	public void checkCanonicalCW09DocumentForDuplicate3() {
		String input = "clueweb09-enwp03-37-22832";
		String expected = "clueweb09-enwp01-47-05055";
		String actual = CanonicalDocuments.canonicalDocument(input);
		
		Assert.assertEquals(expected, actual);
		Assert.assertTrue(CanonicalDocuments.isDuplicate(input));
	}
}
