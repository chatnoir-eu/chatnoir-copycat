package de.webis.cikm20_duplicates.util;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;

import org.apache.commons.io.IOUtils;

import de.webis.WebisUUID;
import de.webis.cikm20_duplicates.spark.SparkEnrichRelevanceTransferPairs;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.index.transform.JsoupStringTransform;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class CollectionDocumentUtil {

	public static void main(String[] args) {
//		// S3: 1.0 (Should be valid)
//		String firstId = "clueweb12-0001wb-23-26353", secondId = "clueweb12-0005wb-24-06245";
		
//		// S3: 0.0 (Should be invalid)
//		String firstId = "clueweb12-0912wb-53-11472", secondId = "clueweb12-1000tw-11-07961";
		
		// S3: 0.0 (Should be invalid)
//		String firstId = "clueweb12-0303wb-10-02360", secondId = "clueweb12-0715wb-02-10722";

//		// S3: 0.0 (Should be invalid)
//		String firstId = "clueweb12-0303wb-35-15203", secondId = "clueweb12-0816wb-90-18920";
		
		// S3: 1.0 (Should be valid)
//		String firstId = "clueweb12-0406wb-47-22712", secondId = "clueweb12-1305wb-37-22239";
		
//		// S3: 1.0 (Should be valid)
//		String firstId = "clueweb12-0308wb-07-19989", secondId = "clueweb12-1505wb-58-23691";
		
//		// S3: 0.899 (Should be valid)
//		String firstId = "clueweb12-0810wb-02-19724", secondId = "clueweb12-0811wb-20-10663";
		
		
//		// S3: 0.99 (Should be valid)
//		String firstId = "clueweb12-0807wb-51-02126", secondId = "clueweb12-0810wb-43-15854";
		
//		// S3: 0.79 (should be invalid)
//		String firstId = "clueweb12-1313wb-20-00368", secondId = "clueweb12-1313wb-24-32697";
		
//		// S3: 0.94 (should be valid)
//		String firstId = "clueweb12-0805wb-13-09655", secondId = "clueweb12-0809wb-63-03994";
		
//		// S3: 0.97 (should be valid)
//		String firstId = "clueweb12-0810wb-79-00932", secondId = "clueweb12-0811wb-54-18364";
		
//		// S3: 0.93 (should be valid)
//		String firstId = "clueweb12-1600tw-18-12106", secondId = "clueweb12-1900tw-30-12707";
		
		// S3: 0.79 (should be invalid)
		String firstId = "clueweb12-1018wb-50-14351", secondId = "clueweb12-1019wb-33-29258";
		
		System.out.println("Retrieve " + firstId + " located at: " + chatNoirURL(firstId) +"&plain");
		CollectionDocument a = loadCollectionDocument(firstId);

		System.out.println("Retrieve " + secondId + " located at: " + chatNoirURL(secondId) +"&plain");
		CollectionDocument b = loadCollectionDocument(secondId);
		
		System.out.println("Calculate S3-Score between " + firstId + " and " + secondId);
		System.out.println(SparkEnrichRelevanceTransferPairs.s3Score(a,b));
	}
	
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
