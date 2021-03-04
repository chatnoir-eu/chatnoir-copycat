package de.webis.copycat.document_preprocessing;

import org.approvaltests.Approvals;
import org.junit.Test;

public class JerichoMainContentDocumentPreprocessingTest extends DocumentPreprocessingTest  {
	public JerichoMainContentDocumentPreprocessingTest() {
		super("--keepStopwords", "True", "--contentExtraction", "Jericho", "--stemmer", "null");
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
