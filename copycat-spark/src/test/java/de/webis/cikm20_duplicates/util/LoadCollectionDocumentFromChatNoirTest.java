package de.webis.cikm20_duplicates.util;

import java.net.URL;

import org.approvaltests.Approvals;
import org.junit.Test;

import lombok.SneakyThrows;
import net.jodah.failsafe.RetryPolicy;

public class LoadCollectionDocumentFromChatNoirTest {

	RetryPolicy<String> NO_RETRIES = new RetryPolicy<String>()
			.handle(Exception.class)
			.withMaxRetries(1);
	
	@SneakyThrows
	@Test(expected = Exception.class)
	public void checkThatNonExistingCw09IdThrowsException() {
		String documentId = "clueweb09-en0004-47-2719612212";
		CollectionDocumentUtil.loadCollectionDocument(new URL(CollectionDocumentUtil.chatNoirURL(documentId)), documentId, NO_RETRIES);
	}

	@SneakyThrows
	@Test(expected = Exception.class)
	public void checkThatNonExistingCw12IdThrowsException() {
		String documentId = "clueweb12-0900wb-15-0870012";
		CollectionDocumentUtil.loadCollectionDocument(new URL(CollectionDocumentUtil.chatNoirURL(documentId)), documentId, NO_RETRIES);
	}
	
	@Test
	public void approveCrawlingOfExampleCw09Document() {
		String documentId = "clueweb09-en0004-47-27196";
		
		Approvals.verify(CollectionDocumentUtil.loadCollectionDocument(documentId));
	}
	
	@Test
	public void approveCrawlingOfExampleCw12Document() {
		String documentId = "clueweb12-0900wb-15-08700";
		
		Approvals.verify(CollectionDocumentUtil.loadCollectionDocument(documentId));
	}
}
