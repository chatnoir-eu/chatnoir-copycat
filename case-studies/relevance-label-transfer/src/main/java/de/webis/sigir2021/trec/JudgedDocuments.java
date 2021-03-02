package de.webis.sigir2021.trec;

import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import de.webis.WebisUUID;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import lombok.SneakyThrows;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.sourceforge.argparse4j.inf.Namespace;

public class JudgedDocuments {

	public static final RetryPolicy<String> RETRY_POLICY = new RetryPolicy<String>()
			.handle(Exception.class)
			.withBackoff(10, 240, ChronoUnit.SECONDS)
			.withMaxRetries(5);
	
	public static String urlOfDocument(String docId) {
		return Failsafe.with(RETRY_POLICY).get(() -> urlOfDocumentThrowsException(docId));
	}
	
	@SneakyThrows
	public static String urlOfDocumentThrowsException(String docId) {
		try(RestHighLevelClient client = client()) {
			GetResponse response = client.get(new GetRequest(index(docId), type(docId), chatNoirId(docId)), RequestOptions.DEFAULT);
			
			if(response.isExists()) {
				return response.getSource().get("warc_target_uri").toString();
			} else {
				return null;
			}
		}
	}
	
	public static String chatNoirId(String documentId) {
		return chatNoirId(longChatNoirId(documentId), documentId).toString();
	}
	
	public static UUID chatNoirId(String prefix, String documentId) {
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
	
	public static String index(String documentId) {
		if (documentId.startsWith("clueweb09")) {
			return "webis_warc_clueweb09_003";
		} else if (documentId.startsWith("clueweb12")) {
			return "webis_warc_clueweb12_011";
		}

		throw new RuntimeException("ID '" + documentId + "' is not supported.");
	}

	public static String type(String documentId) {
		return "warcrecord";
	}
	
	private static RestHighLevelClient client() {
		return new RestHighLevelClient(RestClient.builder(new HttpHost("betaweb023", 9200, "http")));
	}

	public static List<String> judgedDocuments(Namespace parsedArgs) {
		SharedTask task = task(parsedArgs);
		Integer topic = parsedArgs.getInt("topic");
		Set<String> ret = new HashSet<>();
		
		ret.addAll(task.documentJudgments().getRelevantDocuments("" + topic));
		ret.addAll(task.documentJudgments().getIrrelevantDocuments("" + topic));
		
		return ret.stream().sorted().collect(Collectors.toList());
	}
	
	private static SharedTask task(Namespace parsedArgs) {
		return TrecSharedTask.valueOf(parsedArgs.getString("task"));
	}
}
