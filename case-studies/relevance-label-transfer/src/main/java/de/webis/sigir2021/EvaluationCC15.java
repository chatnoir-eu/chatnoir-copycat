package de.webis.sigir2021;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.json.JSONObject;

import de.webis.sigir2021.trec.CanonicalDocuments;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import lombok.SneakyThrows;

public class EvaluationCC15 {

	private static Map<String, Integer> CW09_TO_COUNT_OF_CW12_WITH_SAME_URL;
	
	private static Map<String, Double> CW09_TO_MAX_S3SCORE;

	private static Map<String, Double> CW09_TO_MAX_S3SCORE_DUPLICATE_AWARE;

	private static Map<String, Double> CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE;

	private static Map<String, Double> CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE_DUPLICATE_AWARE;
	
	private EvaluationCC15() {
		if(CW09_TO_COUNT_OF_CW12_WITH_SAME_URL == null) {
			CW09_TO_COUNT_OF_CW12_WITH_SAME_URL = tmp();
			CW09_TO_MAX_S3SCORE = leftDocToMaxS3Score(i -> i);
			CW09_TO_MAX_S3SCORE_DUPLICATE_AWARE = leftDocToMaxS3Score(i -> CanonicalDocuments.canonicalDocument(i));
			CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE = cw09ToMaxNearDuplicateS3Score(i -> i);
			CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE_DUPLICATE_AWARE = cw09ToMaxNearDuplicateS3Score(i -> CanonicalDocuments.canonicalDocument(i));
		}
	}

	public static void main(String[] args) {
		for(String year: new String[] {"2009", "2010", "2011", "2012", "2013", "2014"}) {
			new EvaluationCC15().runEvaluation(year);
		}
	}
	
	@SneakyThrows
	public void runEvaluation(String year) {
		SharedTask task = webTask(year);
		String ret = "";
		for(String topicStr: task.topicNumberToTopic().keySet()) {
			int topic = Integer.parseInt(topicStr);
			
			for(String relevantDoc: task.documentJudgments().getRelevantDocuments(topicStr)) {
				ret += report(topic, relevantDoc, true) +"\n";
			}
			
			for(String irrelevantDoc: task.documentJudgments().getIrrelevantDocuments(topicStr)) {
				ret += report(topic, irrelevantDoc, false) +"\n";
			}
		}
		
		Files.write(new File("cc15-relevance-transfer/web-" + year + ".jsonl").toPath(), ret.getBytes(StandardCharsets.UTF_8));
	}
	
	private String report(int topic, String doc, boolean relevant) {
		String ret = "{\"topic\": " + topic + ",\"document\":\"" + doc +
				"\",\"relevant\":" + relevant + ",\"duplicate\":" + CanonicalDocuments.isDuplicate(doc) +
				",\"urlMatches\":" + CW09_TO_COUNT_OF_CW12_WITH_SAME_URL.getOrDefault(doc, 0) + 
				",\"urlMaxS3Score\":"+ String.format("%.4f",CW09_TO_MAX_S3SCORE.getOrDefault(doc, 0.0)) +
				",\"urlMaxS3ScoreDuplicateAware\":"+ String.format("%.4f",CW09_TO_MAX_S3SCORE_DUPLICATE_AWARE.getOrDefault(CanonicalDocuments.canonicalDocument(doc), 0.0)) +
				",\"maxNearDuplicateS3Score\":" + String.format("%.4f", CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE.getOrDefault(doc, 0d)) +
				",\"maxNearDuplicateS3ScoreDuplicateAware\":" + String.format("%.4f", CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE_DUPLICATE_AWARE.getOrDefault(CanonicalDocuments.canonicalDocument(doc), 0d)) +
			"}";
		System.out.println(ret);
		
		return ret;
	}
	
	private static SharedTask webTask(String year) {
		return TrecSharedTask.valueOf("WEB_" + year);
	}
	
	@SneakyThrows
	private static Map<String, Integer> tmp() {
		Map<String, Integer> ret = new LinkedHashMap<>();
		try(Stream<String> lines = Files.lines(new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/url-transfer-from-cw09-or-cw12-to-cc15-with-similarity.jsonl").toPath(), StandardCharsets.UTF_8)) {
			
			lines.forEach(line -> {
				String sourceId = Evaluation.cw09OrCw12(line);
				String targetId = Evaluation.cc15Id(line);
				
				if(sourceId != null && targetId != null) {
					ret.put(sourceId, ret.getOrDefault(sourceId, 0) +1);
				}
			});
		}
		
		return ret;
	}
	
	@SneakyThrows
	private static Map<String, Double> leftDocToMaxS3Score(Function<String, String> idMapper) {
		Map<String, Double> ret = new LinkedHashMap<>();
		try (Stream<String> lines = Files.lines(new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/url-transfer-from-cw09-or-cw12-to-cc15-with-similarity.jsonl").toPath(), StandardCharsets.UTF_8)) {
			
			lines.forEach(line -> {
				JSONObject json = new JSONObject(line);
				String sourceId = Evaluation.cw09OrCw12(line);
				String targetId = Evaluation.cc15Id(line);
				
				if(sourceId != null && targetId != null) {
					sourceId = idMapper.apply(sourceId);
					Double s3Score = Math.max(json.getDouble("s3Score"), ret.getOrDefault(sourceId, 0.0d));
					ret.put(sourceId, s3Score);
				}
			});
		}
		
		return ret;
	}
	
	@SneakyThrows
	private static Map<String, Double> cw09ToMaxNearDuplicateS3Score(Function<String, String> idMapper) {
		System.out.println("Start cw09ToMaxNearDuplicateS3Score :" + new Date());
		Map<String, Double> ret = new LinkedHashMap<>();
		Stream<String> lines = Files.lines(new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-09-10-2020/relevance-transfer-only-near-duplicates.jsonl").toPath(), StandardCharsets.UTF_8);
		
		lines.forEach(line -> {
			String sourceId = Evaluation.cw09OrCw12(line);
			String targetId = Evaluation.cc15Id(line);
			
			if(sourceId != null && targetId != null) {
				sourceId = idMapper.apply(sourceId);
				Double s3Score = Math.max(Evaluation.s3Score(line), ret.getOrDefault(sourceId, 0.0d));
				ret.put(sourceId, s3Score);
			}
		});
		
		lines = Files.lines(new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-09-10-2020/relevance-transfer-exact-duplicates.jsonl").toPath(), StandardCharsets.UTF_8);
		
		lines.forEach(line -> {
			String sourceId = Evaluation.cw09OrCw12(line);
			String targetId = Evaluation.cc15Id(line);
			
			if(sourceId != null && targetId != null) {
				sourceId = idMapper.apply(sourceId);
				Double s3Score = Math.max(Evaluation.s3Score(line), ret.getOrDefault(sourceId, 0.0d));
				ret.put(sourceId, s3Score);
			}
		});
		
		System.out.println("Finish cw09ToMaxNearDuplicateS3Score :" + new Date());

		return ret;
	}
}
