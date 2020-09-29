package de.webis.cikm20_duplicates.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.json.JSONObject;

import de.webis.WebisUUID;
import de.webis.chatnoir2.webclient.search.DocumentRetriever;
import de.webis.chatnoir2.webclient.search.DocumentRetriever.Document;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.index.transform.JsoupStringTransform;
import lombok.Data;
import lombok.SneakyThrows;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class CollectionDocumentUtil {

	@SneakyThrows
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

//		// S3: 0.79 (should be invalid)
//		String firstId = "clueweb12-1018wb-50-14351", secondId = "clueweb12-1019wb-33-29258";

//		System.out.println("Retrieve " + firstId + " located at: " + chatNoirURL(firstId) +"&plain");
//		CollectionDocument a = loadCollectionDocument(firstId);

//		System.out.println("Retrieve " + secondId + " located at: " + chatNoirURL(secondId) +"&plain");
//		CollectionDocument b = loadCollectionDocument(secondId);

//		System.out.println("Calculate S3-Score between " + firstId + " and " + secondId);
//		System.out.println(SparkEnrichRelevanceTransferPairs.s3Score(a,b));

		// {"equivalentDocuments":
		// ["<urn:uuid:59a4637d-976f-4a5c-9861-cd5f3fc6c737>","<urn:uuid:be66ee99-8ee6-46ce-9e96-de6067eff76a>","<urn:uuid:ecffb099-540a-4176-8a97-7152a0576383>"],"hash":[-244842496,
		// 47039, 11927603, 14439680]}
		// String firstId = "<urn:uuid:59a4637d-976f-4a5c-9861-cd5f3fc6c737>";
		// String secondId = "<urn:uuid:be66ee99-8ee6-46ce-9e96-de6067eff76a>";

		// {"equivalentDocuments":
		// ["<urn:uuid:84606826-df14-4f0d-94cc-7d828f880949>","<urn:uuid:a1a75ce9-255f-4acd-83f9-30afd4fa6631>"],"hash":[-889782272,
		// 50871, 14287080, 7504640]}
		// String firstId = "<urn:uuid:84606826-df14-4f0d-94cc-7d828f880949>";
		// String secondId = "<urn:uuid:a1a75ce9-255f-4acd-83f9-30afd4fa6631>";

		// {"equivalentDocuments":
		// ["<urn:uuid:aa833e3e-0033-4937-b499-14e26d972404>","<urn:uuid:fb1ec340-b003-4dad-aa02-9a3ebe34b75f>"],"hash":[1527775232,
		// 344, 13172802, 9114880]}
		// String firstId = "<urn:uuid:aa833e3e-0033-4937-b499-14e26d972404>";
		// String secondId = "<urn:uuid:fb1ec340-b003-4dad-aa02-9a3ebe34b75f>";

		// {"equivalentDocuments":
		// ["<urn:uuid:177caa19-fd12-48dd-b85f-aa9151e1048e>","<urn:uuid:c74bf9bb-c949-47a1-bd4c-967e528a3e0d>","<urn:uuid:fd3341db-5dbb-45fe-b5c3-7aa1f91527a6>"],"hash":[367919104,
		// 13594, 1900705, 413184]}
//		String firstId = "<urn:uuid:177caa19-fd12-48dd-b85f-aa9151e1048e>";
//		String secondId = "<urn:uuid:fd3341db-5dbb-45fe-b5c3-7aa1f91527a6>";
//		String prefix = "commoncrawl";
//		String index = "cc1704";

		String firstId = "clueweb09-en0000-00-00009";
		String secondId = "clueweb09-en0000-00-00016";
		String prefix = "clueweb09";
		String index = "cw09";

		System.out.println(chatNoirURL(prefix, firstId, index));
//		CollectionDocument a = loadCollectionDocument(firstId, new URL(chatNoirURL(prefix, firstId, index)));
//		System.out.println(chatNoirURL(prefix, secondId, index));
//		CollectionDocument b = loadCollectionDocument(secondId, new URL(chatNoirURL(prefix, secondId, index)));

//		System.out.println(SparkEnrichRelevanceTransferPairs.s3Score(a,b));

		System.out.println(new HdfsMapFileDocumentResolver().loadCollectionDocument(firstId));
		System.out.println(new HdfsMapFileDocumentResolver().loadCollectionDocument(secondId));
		System.out.println(new HdfsMapFileDocumentResolver().loadCollectionDocument("clueweb09-en0000-00-00017c"));
//		System.out.println(new EsDocumentResolver().loadCollectionDocument(firstId));
//		System.out.println(new EsDocumentResolver().loadCollectionDocument(secondId));
	}
	
	public static class HdfsMapFileDocumentResolver {
		
		private final DocumentRetriever documentRetriever = new DocumentRetriever();
		
		private final String indexName = "cw09";
		
		private final String prefix = "clueweb09";
		
		public CollectionDocument loadCollectionDocument(String id) {
			long start = System.currentTimeMillis();
			String raw = raw(id);
			if (raw == null) {
				System.out.println("Retrieving " + id + " took: " + (System.currentTimeMillis() - start));
				return null;
			}

			JsoupStringTransform stringTransform = new JsoupStringTransform();
			CollectionDocument ret = CollectionDocument.collectionDocument(stringTransform.apply(raw), id);
			System.out.println("Retrieving " + id + " took: " + (System.currentTimeMillis() - start));
			return ret;
		}
		
		private String raw(String id) {
			UUID docUUID = webisUUID(prefix, id);
			Document ret = documentRetriever.getByUUID(indexName, docUUID);
			
			return ret == null ? null : ret.getBody();
		}
	}

	@Data
	public static class EsDocumentResolver {
		private final String host = "betaweb023";
		private final int port = 9200;

		public CollectionDocument loadCollectionDocument(String id) {
			long start = System.currentTimeMillis();
			String mainContent = mainContentOfDocument(id);
			if (mainContent == null) {
				return null;
			}

			JsoupStringTransform stringTransform = new JsoupStringTransform();
			CollectionDocument ret = CollectionDocument.collectionDocument(stringTransform.apply(mainContent), id);
			System.out.println(System.currentTimeMillis() - start);
			return ret;
		}

		@SuppressWarnings("unchecked")
		public String mainContentOfDocument(String id) {
			String rawDocument = loadEsDocument(id);
			if(rawDocument == null) {
				return null;
			}
			JSONObject json = new JSONObject(rawDocument);
			JSONObject source = json.getJSONObject("_source");
			List<String> bodyField = ((Set<String>)source.keySet()).stream().filter(i -> i.contains("body_lang"))
					.collect(Collectors.toList());
			if (bodyField.size() < 1) {
				return null;
			}

			return source.getString(bodyField.get(0));
		}

		public String loadEsDocument(String id) {
			return loadEsDocument(id, RETRY_FINAL);
		}

		@SneakyThrows
		public String loadEsDocument(String id, RetryPolicy<String> retryPolicy) {
			id = webisUUID(id);
			URL url = new URL("http://" + host + ":" + port + "/webis_warc_clueweb09_003/warcrecord/" + id);
			if(!documentExists(url)) {
				return null;
			}

			System.out.println(url);
			return Failsafe.with(retryPolicy).get(() -> IOUtils.toString(url, StandardCharsets.UTF_8));
		}

		private static boolean documentExists(URL url) {
			HttpURLConnection urlConnection = null;
		    System.setProperty("http.keepAlive", "false");
		    try {
		        urlConnection = (HttpURLConnection) url.openConnection();
		        urlConnection.setRequestMethod("HEAD");
		        urlConnection.getInputStream().close();
		        return 200 == urlConnection.getResponseCode();
		    } catch (IOException e) {
		    	return false;
		    }
		    finally

		{
			if (urlConnection != null) {
				urlConnection.disconnect();
			}
		}
	}
	}

	public static final RetryPolicy<String> RETRY_FINAL = new RetryPolicy<String>().handle(Exception.class)
			.withBackoff(3, 60, ChronoUnit.SECONDS).withMaxRetries(3);

	@SneakyThrows
	public static CollectionDocument loadCollectionDocument(String documentId) {
		return loadCollectionDocument(new URL(chatNoirURL(documentId)), documentId, RETRY_FINAL);
	}

	public static CollectionDocument loadCollectionDocument(String documentId, URL chatNoirUrl) {
		return loadCollectionDocument(chatNoirUrl, documentId, RETRY_FINAL);
	}

	public static CollectionDocument loadCollectionDocument(URL url, String documentId,
			RetryPolicy<String> retryPolicy) {
		String documentContent = loadRawDocumentFromChatnoir(url, retryPolicy);

		JsoupStringTransform stringTransform = new JsoupStringTransform();
		return CollectionDocument.collectionDocument(stringTransform.apply(documentContent), documentId);
	}

	public static String loadRawDocumentFromChatnoir(URL url, RetryPolicy<String> retryPolicy) {
		return Failsafe.with(retryPolicy).get(() -> IOUtils.toString(url, StandardCharsets.UTF_8));
	}

	private static String webisUUID(String documentId) {
		return webisUUID(longChatNoirId(documentId), documentId).toString();
	}

	private static UUID webisUUID(String prefix, String documentId) {
		return new WebisUUID(prefix).generateUUID(documentId);
	}

	private static String longChatNoirId(String documentId) {
		if (documentId.startsWith("clueweb09")) {
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
		return "https://chatnoir.eu/cache?uuid=" + webisUUID(documentId) + "&index=" + shortChatNoirId(documentId)
				+ "&raw";
	}

	public static String chatNoirURL(String prefix, String documentId, String index) {
		return "https://chatnoir.eu/cache?uuid=" + webisUUID(prefix, documentId) + "&index=" + index + "&raw";
	}
}
