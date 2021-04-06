package de.webis.copycat.document_preprocessing;

import org.approvaltests.Approvals;
import org.junit.Ignore;
import org.junit.Test;

/**
 * See: https://github.com/adbar/trafilatura
 * 
 */
public class TrafilaturaVenvMainContentDocumentPreprocessingTest extends DocumentPreprocessingTest {

	public TrafilaturaVenvMainContentDocumentPreprocessingTest() {
		super("--keepStopwords", "True", "--contentExtraction", "TrafilaturaVenv", "--stemmer", "null");
	}

	@Test
	@Ignore
	public void approveWebisHtmlPage() {
		Approvals.verify(preprocessedHtml("webis-de"));
	}
	
	@Test
	@Ignore
	public void approveToucheHtmlPage() {
		Approvals.verify(preprocessedHtml("touche-webis-de"));
	}
	
	@Test
	@Ignore
	public void approveSigirHtmlPage() {
		Approvals.verify(preprocessedHtml("sigir-org-2021-resource"));		
	}
}
