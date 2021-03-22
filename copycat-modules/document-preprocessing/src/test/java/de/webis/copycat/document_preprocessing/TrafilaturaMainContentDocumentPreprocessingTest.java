package de.webis.copycat.document_preprocessing;

import org.approvaltests.Approvals;
import org.junit.Test;

/**
 * See: https://github.com/adbar/trafilatura
 * 
 */
public class TrafilaturaMainContentDocumentPreprocessingTest extends DocumentPreprocessingTest {

	public TrafilaturaMainContentDocumentPreprocessingTest() {
		super("--keepStopwords", "True", "--contentExtraction", "Trafilatura", "--stemmer", "null");
	}

	@Test
	public void approveWebisHtmlPage() {
		Approvals.verify(preprocessedHtml("webis-de"));
	}
	
	@Test
	public void approveToucheHtmlPage() {
		Approvals.verify(preprocessedHtml("touche-webis-de"));
	}
	
	@Test
	public void approveSigirHtmlPage() {
		Approvals.verify(preprocessedHtml("sigir-org-2021-resource"));		
	}
}
