package de.webis.cikm20_duplicates.util;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;

import org.apache.commons.io.IOUtils;

import de.webis.WebisUUID;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.index.transform.JsoupStringTransform;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class CollectionDocumentUtil {

	private static final RetryPolicy<String> RETRY_FINAL = new RetryPolicy<String>()
			.handle(Exception.class)
			.withBackoff(3, 60, ChronoUnit.SECONDS)
			.withMaxRetries(3);	

	public static CollectionDocument loadCollectionDocument(String documentId) {
		return loadCollectionDocument(documentId, RETRY_FINAL);
	}
	
	public static CollectionDocument loadCollectionDocument(String documentId, RetryPolicy<String> retryPolicy) {
		String documentContent = loadRawDocumentFromChatnoir(documentId, retryPolicy);
		
		JsoupStringTransform stringTransform = new JsoupStringTransform();
		return CollectionDocument.collectionDocument(stringTransform.apply(documentContent), documentId);
	}
	
	public static String loadRawDocumentFromChatnoir(String documentId, RetryPolicy<String> retryPolicy) {
		return Failsafe.with(retryPolicy).get(() -> IOUtils.toString(new URL(chatNoirURL(documentId)), StandardCharsets.UTF_8));
	}

	private static String webisUUID(String documentId) {
		return new WebisUUID(longChatNoirId(documentId))
				.generateUUID(documentId).toString();
	}
	
	
	
	private static String longChatNoirId(String documentId) {
		if(documentId.startsWith("clueweb09")) {
			return "clueweb09";
		} else if (documentId.startsWith("clueweb12")) {
			return "clueweb12";
		}
		
		throw new RuntimeException("ID '" + documentId + "' is not supported.");
	}
	
	private static String shortChatNoirId(String documentId) {
		if (documentId.startsWith("clueweb09")) {
			return "cw09";
		} else if (documentId.startsWith("clueweb12")) {
			return "cw12";
		}
		
		throw new RuntimeException("ID '" + documentId + "' is not supported.");
	}
	
	public static String chatNoirURL(String documentId) {
		return "https://chatnoir.eu/cache?uuid=" + webisUUID(documentId) + "&index="+ shortChatNoirId(documentId) +"&raw";
	}
}
