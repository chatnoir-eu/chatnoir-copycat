package de.webis.sigir2021.trec;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

import de.webis.sigir2021.Evaluation;
import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

public class LabelTransfer {

	static final double S3_THRESHOLD = 0.82;
	
	public static void main(String[] args) {
		Path baseDir = Paths.get("src/main/resources/artificial-qrels");
		
		Map<String, List<TargetDocument>> urlDuplicates = leftDocToMaxS3Score();
		Map<String, List<TargetDocument>> simHashNearDuplicates = cw09ToMaxNearDuplicateS3Score();
		List<Map<String, List<TargetDocument>>> clueWeb12Target = Arrays.asList(urlDuplicates, simHashNearDuplicates);
		Map<String, List<TargetDocument>> waybackNearDuplicates = waybackNearDuplicates(Evaluation.WAYBACK_SIMILARITIES_FILE);
		List<Map<String, List<TargetDocument>>> cluewebAndWaybackTarget = Arrays.asList(urlDuplicates, simHashNearDuplicates, waybackNearDuplicates);
		
		writeRelevanceTransferQrels(TrecSharedTask.WEB_2009, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12.1-50.jsonl"), clueWeb12Target);
		writeRelevanceTransferQrels(TrecSharedTask.WEB_2010, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12.51-100.jsonl"), clueWeb12Target);
		writeRelevanceTransferQrels(TrecSharedTask.WEB_2011, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12.101-150.jsonl"), clueWeb12Target);
		writeRelevanceTransferQrels(TrecSharedTask.WEB_2012, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12.151-200.jsonl"), clueWeb12Target);
		
		writeRelevanceTransferQrels(TrecSharedTask.WEB_2009, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12wb12.1-50.jsonl"), cluewebAndWaybackTarget);
		writeRelevanceTransferQrels(TrecSharedTask.WEB_2010, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12wb12.51-100.jsonl"), cluewebAndWaybackTarget);
		writeRelevanceTransferQrels(TrecSharedTask.WEB_2011, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12wb12.101-150.jsonl"), cluewebAndWaybackTarget);
		writeRelevanceTransferQrels(TrecSharedTask.WEB_2012, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12wb12.151-200.jsonl"), cluewebAndWaybackTarget);
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TargetDocument {
		private String targetId;
		private double s3Score;
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RelevanceTransfer {
		private int relevanceLabel;
		private int topicNumber;
		private String sourceDocument;
		private List<TargetDocument> targetDocs;
		
		@SneakyThrows
		public static RelevanceTransfer fromString(String src) {
			System.out.println(src);
			return new ObjectMapper().readValue(src, RelevanceTransfer.class);
		}
	}
	
	@SneakyThrows
	static void writeRelevanceTransferQrels(SharedTask task, Path path, List<Map<String, List<TargetDocument>>> targetDocsForDocuments) {
		String ret = task.getQrelResourcesWithoutScore().stream()
				.map(i -> targetDocsForQrel(i, targetDocsForDocuments))
				.filter(i -> i != null)
				.collect(Collectors.joining("\n"));
		Files.write(path, ret.getBytes());
	}
	
	@SneakyThrows
	private static String targetDocsForQrel(QrelEqualWithoutScore qrel, List<Map<String, List<TargetDocument>>> targetDocsForDocuments) {
		if(CanonicalDocuments.isDuplicate(qrel.getDocumentID())) {
			return null;
		}
		
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("relevanceLabel", qrel.getScore());
		ret.put("topicNumber", qrel.getTopicNumber());
		ret.put("sourceDocument", qrel.getDocumentID());
		List<TargetDocument> targetDocs = new ArrayList<>();
		for(Map<String, List<TargetDocument>> t: targetDocsForDocuments) {
			targetDocs.addAll(t.getOrDefault(qrel.getDocumentID(), new ArrayList<>()));
		}
		
		ret.put("targetDocs", targetDocs);
		
		return new ObjectMapper().writeValueAsString(ret);
	}
	
	@SneakyThrows
	private static Map<String, List<TargetDocument>> leftDocToMaxS3Score() {
		Map<String, List<TargetDocument>> ret = new LinkedHashMap<>();
		try (Stream<String> lines = Files.lines(new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-05-10-2020/url-transfer-from-cw09-to-cw12-with-similarity.jsonl").toPath(), StandardCharsets.UTF_8)) {
			
			lines.forEach(line -> {
				JSONObject json = new JSONObject(line);
				String cw09Id = CanonicalDocuments.canonicalDocument(json.getString("firstId"));
				
				double s3Score = json.getDouble("s3Score");
				
				if(S3_THRESHOLD <= s3Score) {
					if(!ret.containsKey(cw09Id)) {
						ret.put(cw09Id, new ArrayList<>());
					}
					
					ret.get(cw09Id).add(new TargetDocument(json.getString("secondId"), s3Score));
				}
			});
		}
		
		return ret;
	}

	@SneakyThrows
	private static Map<String, List<TargetDocument>> cw09ToMaxNearDuplicateS3Score() {
		System.out.println("Start cw09ToMaxNearDuplicateS3Score :" + new Date());
		Map<String, List<TargetDocument>> ret = new LinkedHashMap<>();
		Stream<String> lines = Files.lines(new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-09-10-2020/relevance-transfer-only-near-duplicates.jsonl").toPath(), StandardCharsets.UTF_8);
		
		lines.forEach(line -> {
			String cw09Id = Evaluation.cw09Id(line);
			String cw12Id = Evaluation.cw12Id(line);
			
			if(cw09Id != null && cw12Id != null) {
				cw09Id = CanonicalDocuments.canonicalDocument(cw09Id);
				
				Double s3Score = Evaluation.s3Score(line);
				if(S3_THRESHOLD <= s3Score) {
					if(!ret.containsKey(cw09Id)) {
						ret.put(cw09Id, new ArrayList<>());
					}
					ret.get(cw09Id).add(new TargetDocument(cw12Id, s3Score));
				}
			}
		});
		
		lines = Files.lines(new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-09-10-2020/relevance-transfer-exact-duplicates.jsonl").toPath(), StandardCharsets.UTF_8);
		
		lines.forEach(line -> {
			String cw09Id = Evaluation.cw09Id(line);
			String cw12Id = Evaluation.cw12Id(line);
			
			if(cw09Id != null && cw12Id != null) {
				cw09Id = CanonicalDocuments.canonicalDocument(cw09Id);
				
				Double s3Score = Evaluation.s3Score(line);
				if(S3_THRESHOLD <= s3Score) {
					if(!ret.containsKey(cw09Id)) {
						ret.put(cw09Id, new ArrayList<>());
					}
					ret.get(cw09Id).add(new TargetDocument(cw12Id, s3Score));
				}
			}
		});
		
		System.out.println("Finish cw09ToMaxNearDuplicateS3Score :" + new Date());

		return ret;
	}

	@SneakyThrows
	public static Map<String, List<TargetDocument>> waybackNearDuplicates(String input) {
		try (Stream<String> lines = Files.lines(new File(input).toPath(), StandardCharsets.UTF_8)) {
			Map<String, List<TargetDocument>> ret = new LinkedHashMap<>();
			
			lines.forEach(line -> {
				JSONObject json = new JSONObject(line);
				String cw09Id = json.getString("firstId");
				cw09Id = CanonicalDocuments.canonicalDocument(cw09Id);
				String targetId = json.getString("secondId");
				
				Double s3Score = json.getDouble("s3Score");
				if(S3_THRESHOLD <= s3Score) {
					if(!ret.containsKey(cw09Id)) {
						ret.put(cw09Id, new ArrayList<>());
					}
					
					ret.get(cw09Id).add(new TargetDocument(targetId, s3Score));
				}
			});
			
			return ret;
		}
	}
}
