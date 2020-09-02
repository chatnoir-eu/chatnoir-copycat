package de.webis.cikm20_duplicates.util;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;

import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.experimental.Wither;

/**
 * 
 * @author Maik Fröbe
 *
 */
@UtilityClass
public final class SourceDocuments {
	private static final List<TrecSharedTask> TRACKS = Arrays.asList(
			TrecSharedTask.WEB_2009, TrecSharedTask.WEB_2010,
			TrecSharedTask.WEB_2011, TrecSharedTask.WEB_2012,

			TrecSharedTask.WEB_2013, TrecSharedTask.WEB_2014,

			TrecSharedTask.SESSION_2010, TrecSharedTask.SESSION_2011,
			TrecSharedTask.SESSION_2012,

			TrecSharedTask.SESSION_2013, TrecSharedTask.SESSION_2014,
			
			TrecSharedTask.ENTITY_2009, TrecSharedTask.ENTITY_2010,
			TrecSharedTask.ENTITY_2011
	);
	
	public static final List<SourceDocument> ALL_DOCS_FOR_WHICH_DUPLICATES_SHOULD_BE_SEARCHED = getAllDocumentsForWhichDuplicatesShouldBeSearched();
	
	public static final Map<String, Map<String, SourceDocument>> TOPIC_TO_ID_TO_SOURCE_DOC = topicsToSourceDocuments();
	
	@Data
	@Wither
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class CollectionDocumentWithTopics implements Serializable {
		private CollectionDocument doc;
		private List<String> topics;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
		
		@SneakyThrows
		public static CollectionDocumentWithTopics fromString(String source) {
			return new ObjectMapper().readValue(source, CollectionDocumentWithTopics.class);
		}
	}
	
	@Data
	@Wither
	@AllArgsConstructor
	public static class SourceDocument {
		private final TrecSharedTask task;
		private final TrecCollections collection;
		private final String documentId;
		private final String topic;
		private final int judgment;
		
		public String toString() {
			return collection.name() + "::" + task.name() + "::" + topic + "::" + documentId;
		}
	}
	
	@Data
	@Wither
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class DocumentWithFingerprint implements Serializable {
		private String docId;
		private URL url;
		private URL canonicalURL;
		private LinkedHashMap<String, ArrayList<Integer>> fingerprints;
		private String crawlingTimestamp;
		private int documentLengthInWords;
		private int documentLengthInWordsOnFullyCanonicalizedContent;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}

		@SneakyThrows
		public static DocumentWithFingerprint fromString(String src) {
			return new ObjectMapper().readValue(src, DocumentWithFingerprint.class);
		}
	}

	private static List<SourceDocument> getAllDocumentsForWhichDuplicatesShouldBeSearched() {
		List<SourceDocument> ret = new LinkedList<>();
		for (SharedTask task : TRACKS) {
			Set<String> topics = task.documentJudgments().getData().keySet();

			for (String topic : topics) {
				Map<String, String> judgmentsForTopic = task.documentJudgments().getData().get(topic);
				Set<String> judgedDocs = judgmentsForTopic.keySet();
				for (String doc : judgedDocs) {
					ret.add(new SourceDocument(
							(TrecSharedTask) task, collectionForTask(task),
							doc, topic, Integer.parseInt(judgmentsForTopic.get(doc))
					));
				}
			}
		}

		Collections.sort(ret, (a,b) -> a.toString().compareTo(b.toString()));
		
		return Collections.unmodifiableList(ret);
	}

	public static Map<TrecSharedTask, List<SourceDocument>> taskToDocuments() {
		Map<TrecSharedTask, List<SourceDocument>> ret = new LinkedHashMap<>();
		
		for(SourceDocument doc : ALL_DOCS_FOR_WHICH_DUPLICATES_SHOULD_BE_SEARCHED) {
			if(!ret.containsKey(doc.getTask())) {
				ret.put(doc.getTask(), new LinkedList<>());
			}
			
			ret.get(doc.getTask()).add(doc);
		}
		
		return ret;
	}
	

	private static Map<String, Map<String, SourceDocument>> topicsToSourceDocuments() {
		Map<String, Map<String, SourceDocument>> ret = new LinkedHashMap<>();
		for(SourceDocument doc : ALL_DOCS_FOR_WHICH_DUPLICATES_SHOULD_BE_SEARCHED) {
			String topic = topicName(doc);
			
			if(!ret.containsKey(topic)) {
				ret.put(topic, new LinkedHashMap<>());
			}
			
			ret.get(topic).put(doc.documentId, doc);
		}
		
		return ret;
	}
	
	public static Map<String, Set<String>> topicsToDocumentIds() {
		Map<String, Set<String>> ret = new LinkedHashMap<>();
		
		for(SourceDocument doc : ALL_DOCS_FOR_WHICH_DUPLICATES_SHOULD_BE_SEARCHED) {
			String topic = topicName(doc);
			if(!ret.containsKey(topic)) {
				ret.put(topic, new HashSet<>());
			}
			
			ret.get(topic).add(doc.documentId);
		}
		
		return ret;
	}
	
	private static String topicName(SourceDocument doc) {
		return doc.collection.name() + "::" + doc.task.name() + "::" + doc.topic;
	}
	
	private static TrecCollections collectionForTask(SharedTask task) {
		if(Arrays.asList(TrecSharedTask.WEB_2009, TrecSharedTask.WEB_2010,
			TrecSharedTask.WEB_2011, TrecSharedTask.WEB_2012).contains(task)) {
			return TrecCollections.CLUEWEB09;
		} else if(Arrays.asList(TrecSharedTask.WEB_2013, TrecSharedTask.WEB_2014).contains(task)) {
			return TrecCollections.CLUEWEB12;
		} else if(Arrays.asList(TrecSharedTask.SESSION_2010, TrecSharedTask.SESSION_2011,
				TrecSharedTask.SESSION_2012).contains(task)) {
			return TrecCollections.CLUEWEB09;
		} else if(Arrays.asList(TrecSharedTask.SESSION_2013, TrecSharedTask.SESSION_2014).contains(task)) {
			return TrecCollections.CLUEWEB12;
		} else if (Arrays.asList(TrecSharedTask.ENTITY_2009, TrecSharedTask.ENTITY_2010,
			TrecSharedTask.ENTITY_2011).contains(task)) {
			return TrecCollections.CLUEWEB09;
		}
		
		throw new RuntimeException("fixme");
	}
}
