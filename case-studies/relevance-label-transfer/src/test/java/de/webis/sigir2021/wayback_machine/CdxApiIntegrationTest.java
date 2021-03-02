package de.webis.sigir2021.wayback_machine;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.sigir2021.wayback_machine.CdxApi;

public class CdxApiIntegrationTest {
	@Test
	public void approveWaybackSnapshotsForUrlWebis() {
		String actual = CdxApi.snapshotsForUrlBetween2009And2013("http://webis.de");
		
		Approvals.verify(actual);
	}
	
	@Test
	public void approveWaybackSnapshotsForUrlWebisWithHttpsProtocoll() {
		String actual = CdxApi.snapshotsForUrlBetween2009And2013("https://webis.de");
		
		Approvals.verify(actual);
	}
	
	@Test
	public void approveWaybackSnapshotsForUrlWebisWithHttpsProtocollAndTrailingBackslash() {
		String actual = CdxApi.snapshotsForUrlBetween2009And2013("https://webis.de/");
		
		Approvals.verify(actual);
	}
	
	@Test
	public void approveDogBreedsWikipediaArticle() {
		String actual = CdxApi.snapshotsForUrlBetween2009And2013("https://en.wikipedia.org/wiki/Dog_breeds");
		
		Approvals.verify(actual);
	}
	
	@Test
	public void approveDocumentFromWeb2009Track() {
		String actual = CdxApi.snapshotsForUrlBetween2009And2013("http://www.cellphonenews.com/category/usa_canada_phones/3g_cell_phones");
		
		Approvals.verify(actual);
	}
	
	@Test
	public void approveDocumentFromWeb2011Track() {
		String actual = CdxApi.snapshotsForUrlBetween2009And2013("http://www.tottevents.com/index.php/Virtual-Reality/Laser-Skeet-Shooting");
		
		Approvals.verify(actual);
	}
	
	@Test
	public void approveDocumentWithWhitespace() {
		String actual = CdxApi.snapshotsForUrlBetween2009And2013("http://en.wikipedia.org/wiki/Biloxi, MS");
		
		Approvals.verify(actual);
	}
	
	@Test
	public void approveDocumentWithWhitespace2() {
		String actual = CdxApi.snapshotsForUrlBetween2009And2013("http://en.wikipedia.org/wiki/Biloxi, Mississippi");
		
		Approvals.verify(actual);
	}
}
