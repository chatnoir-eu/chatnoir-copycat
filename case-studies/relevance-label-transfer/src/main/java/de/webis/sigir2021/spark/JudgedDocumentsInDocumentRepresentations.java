package de.webis.sigir2021.spark;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import de.webis.sigir2021.trec.JudgedDocuments;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import lombok.SneakyThrows;
import scala.Tuple2;
import scala.Tuple3;

public class JudgedDocumentsInDocumentRepresentations {
	
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			Map<String, String> docIdToUrl = docIdToUrl(context, judgedCW09WebDocuments(), 25);
			Map<String, String> secondDocToUrl = docIdToUrl(context.textFile("sigir2021/cw09-repartitioned"), docIdToUrl);
			JavaRDD<String> cw12DocumentRepresentations = context.textFile("sigir2021/cw12-repartitioned");
			
			Map<String, List<String>> urlToDocId = new LinkedHashMap<>();
			
			for(Map.Entry<String, List<String>> tmp : urltoDocId(docIdToUrl).entrySet()) {
				if(!urlToDocId.containsKey(tmp.getKey())) {
					urlToDocId.put(tmp.getKey(), new ArrayList<>());
				}
				
				urlToDocId.get(tmp.getKey()).addAll(tmp.getValue());
			}
			
			for(Map.Entry<String, List<String>> tmp : urltoDocId(secondDocToUrl).entrySet()) {
				if(!urlToDocId.containsKey(tmp.getKey())) {
					urlToDocId.put(tmp.getKey(), new ArrayList<>());
				}
				
				urlToDocId.get(tmp.getKey()).addAll(tmp.getValue());
			}
			
			urlSearch2(urlToDocId, cw12DocumentRepresentations)
				.saveAsTextFile("sigir2021/url-judgments-from-cw09-to-cw12");
		}
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("JudgedDocumentsInDocumentRepresentations");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> urlSearch(Map<String, String> docIdToUrl, JavaRDD<String> documentRepresentations) {
		return urlSearch2(urltoDocId(docIdToUrl), documentRepresentations);
	}
	
	public static JavaRDD<String> urlSearch2(Map<String, List<String>> urlToDocId, JavaRDD<String> documentRepresentations) {
		JavaRDD<Tuple3<String, String, String>> idUrlCanonicalURL = documentRepresentations.map(i -> parseDoc(i));
		
		return idUrlCanonicalURL.flatMapToPair(i -> joinSourceData(i, urlToDocId))
				.groupByKey()
				.map(i -> tmp(i));
	}

	@SneakyThrows
	private static String tmp(Tuple2<String, Iterable<String>> i) {
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("sourceId", i._1());
		ret.put("targetIds", ImmutableList.copyOf(i._2));
		
		return new ObjectMapper().writeValueAsString(ret);
	}

	private static Iterator<Tuple2<String, String>> joinSourceData(Tuple3<String, String, String> i, Map<String, List<String>> urlToDocId) {
		String targetId = i._1();
		Set<String> sourceIds = sourceIds(i._2(), urlToDocId);
		sourceIds.addAll(sourceIds(i._3(), urlToDocId));
		
		return sourceIds.stream().map(sourceId -> new Tuple2<>(sourceId, targetId)).iterator();
	}

	private static Set<String> sourceIds(String url, Map<String, List<String>> urlToDocId) {
		Set<String> ret = new HashSet<>();
		
		if(urlToDocId.containsKey(url)) {
			ret.addAll(urlToDocId.get(url));
		}
		
		if(urlToDocId.containsKey(normalizeProtocol(url))) {
			ret.addAll(urlToDocId.get(normalizeProtocol(url)));
		}
		
		if(urlToDocId.containsKey(normalizeProtocolAndEncoding(url))) {
			ret.addAll(urlToDocId.get(normalizeProtocolAndEncoding(url)));
		}

		if(urlToDocId.containsKey(reverseNormalizeProtocolAndEncoding(url))) {
			ret.addAll(urlToDocId.get(reverseNormalizeProtocolAndEncoding(url)));
		}
		
		return ret;
	}

	private static Tuple3<String, String, String> parseDoc(String json) {
		try {
			JSONObject parsedJson = new JSONObject(json);
			
			return new Tuple3<>(
				parsedJson.getString("docId"),
				parsedJson.has("url") && !parsedJson.isNull("url") ? parsedJson.getString("url") : null,
				parsedJson.has("canonicalURL") && !parsedJson.isNull("canonicalURL") ? parsedJson.getString("canonicalURL") : null
			);
		} catch(Exception e) {
			throw new RuntimeException(json);
		}
	}
	
	static Map<String, List<String>> urltoDocId(Map<String, String> docIdToUrl) {
		Map<String, List<String>> ret = new LinkedHashMap<>();
		
		for(Map.Entry<String, String> idToUrl: docIdToUrl.entrySet()) {
			if(idToUrl.getValue() == null) {
				continue;
			}
			
			String url = normalizeProtocol(idToUrl.getValue());
			
			if(!ret.containsKey(url)) {
				ret.put(url, new ArrayList<>());
			}
			
			ret.get(url).add(idToUrl.getKey());
		}
		
		return ret;
	}
	
	@SneakyThrows
	private static String normalizeProtocol(String url) {
		if(url == null) {
			return url;
		}
		
		if(url.startsWith("https://")) {
			url = StringUtils.replaceFirst(url, "https://", "");
		}
		if(url.startsWith("http://")) {
			url = StringUtils.replaceFirst(url, "http://", "");
		}
		
		return url;
	}
	
	@SneakyThrows
	private static String normalizeProtocolAndEncoding(String url) {
		if(url == null) {
			return url;
		}

		return StringUtils.replaceAll(normalizeProtocol(url), " ", "%20");
	}
	
	@SneakyThrows
	private static String reverseNormalizeProtocolAndEncoding(String url) {
		if(url == null) {
			return url;
		}

		return StringUtils.replaceAll(normalizeProtocol(url), "%20", " ");
	}

	public static List<String> judgedCW09WebDocuments() {
		Set<String> ret = new HashSet<>();
		
		for(SharedTask task: new SharedTask[] {TrecSharedTask.WEB_2009, TrecSharedTask.WEB_2010, TrecSharedTask.WEB_2011, TrecSharedTask.WEB_2012}) {
			for(String topic: task.documentJudgments().topics()) {
				ret.addAll(task.documentJudgments().getIrrelevantDocuments(topic));
				ret.addAll(task.documentJudgments().getRelevantDocuments(topic));
			}
		}

		return ret.stream().sorted().collect(Collectors.toList());
	}

	public static Map<String, String> docIdToUrl(JavaSparkContext context, List<String> input, int numSlices) {
		Map<String, String> ret = context.parallelize(input, numSlices).mapToPair(i -> new Tuple2<>(i, JudgedDocuments.urlOfDocument(i)))
				.filter(i -> i._2() != null && i._1() != null)
				.collectAsMap();
		
		return new LinkedHashMap<>(ret.keySet().stream().sorted().collect(Collectors.toMap(i -> i, i -> ret.get(i))));
	}

	public static Map<String, String> docIdToUrl(JavaRDD<String> testDocumentsWithoutCanonicalLinks, Map<String, String> docIdToUrl) {
		Set<String> ids = new HashSet<>(docIdToUrl.keySet());
		
		return new LinkedHashMap<>(testDocumentsWithoutCanonicalLinks.map(i -> parseDoc(i))
				.filter(i -> i._2() != null)
				.filter(i -> ids.contains(i._1()))
				.mapToPair(i -> new Tuple2<>(i._1(), i._2()))
				.collectAsMap());
	}
}
