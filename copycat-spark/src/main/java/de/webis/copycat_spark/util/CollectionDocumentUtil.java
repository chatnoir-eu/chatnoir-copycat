package de.webis.copycat_spark.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import de.aitools.ir.fingerprinting.representation.HashVector;
import de.aitools.ir.fingerprinting.representation.HashVectorSha3;
import de.webis.WebisUUID;
import de.webis.chatnoir2.webclient.search.DocumentRetriever;
import de.webis.chatnoir2.webclient.search.DocumentRetriever.Document;
import de.webis.copycat.DocumentResolver;
import de.webis.copycat_spark.app.ArgumentParsingUtil;
import de.webis.copycat_spark.spark.SparkCanonicalLinkGraphExtraction;
import de.webis.copycat_spark.spark.SparkCreateSourceDocuments;
import de.webis.copycat_spark.spark.SparkEnrichRelevanceTransferPairs;
import de.webis.copycat_spark.spark.eval.SparkEvaluateSimHashFeatures;
import de.webis.copycat_spark.util.SourceDocuments.DocumentWithFingerprint;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.index.transform.JsoupStringTransform;
import lombok.Data;
import lombok.SneakyThrows;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.sourceforge.argparse4j.inf.Namespace;

public class CollectionDocumentUtil {

	@SneakyThrows
	public static void main(String[] args) {
		String firstId = "clueweb09-en0002-17-16080";
		String secondId = "clueweb09-en0009-61-13707";
		String prefix = "clueweb09";
		String index = "cw09";

		System.out.println(firstId + " --> " + chatNoirURL(prefix, firstId, index));
		System.out.println(secondId + " --> " + chatNoirURL(prefix, secondId, index));
		CollectionDocument a = new HdfsMapFileDocumentResolver(index, prefix).loadCollectionDocument(firstId);
//		System.out.println(chatNoirURL(prefix, secondId, index));
		CollectionDocument b = new HdfsMapFileDocumentResolver(index, prefix).loadCollectionDocument(secondId);

		System.out.println(SparkEnrichRelevanceTransferPairs.s3Score(a, b));

		DocumentWithFingerprint aFP = SparkCreateSourceDocuments.fingerprintDocument(a,
				SparkCreateSourceDocuments.PRODUCTION_FINGERPRINTS);
		DocumentWithFingerprint bFP = SparkCreateSourceDocuments.fingerprintDocument(b,
				SparkCreateSourceDocuments.PRODUCTION_FINGERPRINTS);

		HashVector aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.theeAndFiveGramms(a), 64);
		HashVector bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.theeAndFiveGramms(b), 64);
		System.out.println("Cosine Similarity (3+5 grams): " + aVec.getCosSimilarity(bVec));
		System.out.println("Fingerprint Doc a: " + aFP.getFingerprints());
		System.out.println("Fingerprint Doc b: " + bFP.getFingerprints());

		aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(a, 8), 64);
		bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(b, 8), 64);

		System.out.println("Cosine Similarity (8 grams): " + aVec.getCosSimilarity(bVec));

		// FIXME: Check the cosine similarity of those vectors with external tool.
		aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(a, 1), 64);
		bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(b, 1), 64);

		System.out.println("Cosine Similarity (1 grams): " + aVec.getCosSimilarity(bVec));
	}

	@Data
	public static class HdfsMapFileDocumentResolver implements DocumentResolver {

		private final DocumentRetriever documentRetriever = new DocumentRetriever();

		private final String indexName;

		private final String prefix;

		public static HdfsMapFileDocumentResolver fromArgs(Namespace args) {
			return fromArgs(args.getAttrs());
		}
		
		public static HdfsMapFileDocumentResolver fromArgs(Map<String, Object> args) {
			return new HdfsMapFileDocumentResolver(
				(String) args.get(ArgumentParsingUtil.UUID_INDEX),
				(String) args.get(ArgumentParsingUtil.UUID_PREFIX)
			);
		}

		@Override
		public CollectionDocument loadCollectionDocument(String id) {
			long start = System.currentTimeMillis();
			Document doc = doc(id);
			if (doc == null) {
				System.out.println("Retrieving " + id + " took: " + (System.currentTimeMillis() - start));
				return null;
			}
			
			String raw = doc.getBody();
			
			if(raw == null || raw.trim().isEmpty()) {
				System.out.println("Retrieving " + id + " took: " + (System.currentTimeMillis() - start));
				return null;
			}

			JsoupStringTransform stringTransform = new JsoupStringTransform();
			CollectionDocument ret = null;
			try {
				ret = CollectionDocument.collectionDocument(stringTransform.apply(raw), id);
			} catch(Exception e) {
				System.out.println("Retrieving " + id + " took: " + (System.currentTimeMillis() - start));
				return null;
			}
			
			ret.setCanonicalUrl(SparkCanonicalLinkGraphExtraction.extractCanonicalLinkOrNull(doc.getTargetURI(), raw));
			ret.setUrl(urlOrNull(doc.getTargetURI()));
			
			System.out.println("Retrieving " + id + " took: " + (System.currentTimeMillis() - start));
			return ret;
		}

		private URL urlOrNull(String url) {
			try {
				return new URL(url);
			} catch (MalformedURLException e) {
				return null;
			}
		}
		
		private Document doc(String id) {
			UUID docUUID = webisUUID(prefix, id);
			return documentRetriever.getByUUID(indexName, docUUID);
		}

		public static DocumentResolver smartDocumentResolver() {
			return new DocumentResolver() {
				@Override
				public CollectionDocument loadCollectionDocument(String id) {
					Map<String, Object> config = config(id);
					
					return HdfsMapFileDocumentResolver.fromArgs(config).loadCollectionDocument(id);
				}
				
				private Map<String, Object> config(String id) {
					Map<String, Object> ret = new HashMap<>();
					
					ret.put(ArgumentParsingUtil.UUID_INDEX, shortChatNoirId(id));
					ret.put(ArgumentParsingUtil.UUID_PREFIX, longChatNoirId(id));
					
					return ret;
				}
			};
		}
	}

	@Data
	public static class EsDocumentResolver implements DocumentResolver {
		private final String host = "betaweb023";
		private final int port = 9200;

		@Override
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
			if (rawDocument == null) {
				return null;
			}
			JSONObject json = new JSONObject(rawDocument);
			JSONObject source = json.getJSONObject("_source");
			List<String> bodyField = ((Set<String>) source.keySet()).stream().filter(i -> i.contains("body_lang"))
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
			if (!documentExists(url)) {
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
			} finally

			{
				if (urlConnection != null) {
					urlConnection.disconnect();
				}
			}
		}
	}
	
	@Data
	public static class ChatNoirDocumentResolver implements DocumentResolver {
		private final String indexName, prefix;
		
		@Override
		@SneakyThrows
		public CollectionDocument loadCollectionDocument(String id) {
			URL chatNoirUrl = new URL(CollectionDocumentUtil.chatNoirURL(prefix, id, indexName));
			return CollectionDocumentUtil.loadCollectionDocument(id, chatNoirUrl);
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
		} else {
			return "commoncrawl";
		}
	}

	private static String shortChatNoirId(String documentId) {
		if (documentId.startsWith("clueweb09")) {
			return "cw09";
		} else if (documentId.startsWith("clueweb12")) {
			return "cw12";
		} else {
			return "cc1511";
		}
	}

	public static String chatNoirURL(String documentId) {
		return "https://chatnoir.eu/cache?uuid=" + webisUUID(documentId) + "&index=" + shortChatNoirId(documentId)
				+ "&raw";
	}

	public static String chatNoirURL(String prefix, String documentId, String index) {
		return "https://chatnoir.eu/cache?uuid=" + webisUUID(prefix, documentId) + "&index=" + index + "&raw";
	}
}
