package de.webis.sigir2021.spark;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;

public class JudgedDocumentsInDocumentRepresentationsCC15 {

	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			Map<String, String> docIdToUrl = JudgedDocumentsInDocumentRepresentations.docIdToUrl(context, judgedCW09AndCW12Documents(), 25);
			JavaRDD<String> sourceDocs = context.textFile("sigir2021/{cw09,cw12}-repartitioned");
			
			Map<String, String> secondDocToUrl = JudgedDocumentsInDocumentRepresentations.docIdToUrl(sourceDocs, docIdToUrl);
			Map<String, List<String>> urlToDocId = new LinkedHashMap<>();
			
			for(Map.Entry<String, List<String>> tmp : JudgedDocumentsInDocumentRepresentations.urltoDocId(docIdToUrl).entrySet()) {
				if(!urlToDocId.containsKey(tmp.getKey())) {
					urlToDocId.put(tmp.getKey(), new ArrayList<>());
				}
				
				urlToDocId.get(tmp.getKey()).addAll(tmp.getValue());
			}
			
			for(Map.Entry<String, List<String>> tmp : JudgedDocumentsInDocumentRepresentations.urltoDocId(secondDocToUrl).entrySet()) {
				if(!urlToDocId.containsKey(tmp.getKey())) {
					urlToDocId.put(tmp.getKey(), new ArrayList<>());
				}
				
				urlToDocId.get(tmp.getKey()).addAll(tmp.getValue());
			}
			
			JavaRDD<String> cc15DocumentRepresentations = context.textFile("sigir2021/cc-2015-11-repartitioned");
			JudgedDocumentsInDocumentRepresentations.urlSearch2(urlToDocId, cc15DocumentRepresentations)
				.saveAsTextFile("sigir2021/url-judgments-from-cw09-or-cw12-to-cc-2015-11");
		}
	}
	
	public static List<String> judgedCW09AndCW12Documents() {
		Set<String> ret = new HashSet<>();
		
		for(SharedTask task: new SharedTask[] {TrecSharedTask.WEB_2009, TrecSharedTask.WEB_2010, TrecSharedTask.WEB_2011, TrecSharedTask.WEB_2012, TrecSharedTask.WEB_2013, TrecSharedTask.WEB_2014}) {
			for(String topic: task.documentJudgments().topics()) {
				ret.addAll(task.documentJudgments().getIrrelevantDocuments(topic));
				ret.addAll(task.documentJudgments().getRelevantDocuments(topic));
			}
		}

		return ret.stream().sorted().collect(Collectors.toList());
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("JudgedDocumentsInDocumentRepresentations");

		return new JavaSparkContext(conf);
	}
}
