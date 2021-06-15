package de.webis.copycat.document_preprocessing;

import org.approvaltests.Approvals;
import org.junit.Test;

/**
 * See: https://pypi.org/project/jusText/
 * 
 */
public class JustextVenvMainContentDocumentPreprocessingTest extends DocumentPreprocessingTest {

	public JustextVenvMainContentDocumentPreprocessingTest() {
		super("--keepStopwords", "True", "--contentExtraction", "JustextVenv", "--stemmer", "null");
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
