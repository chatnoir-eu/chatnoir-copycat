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
import java.util.stream.Stream;

import org.json.JSONObject;

import de.webis.sigir2021.Evaluation;
import de.webis.sigir2021.trec.LabelTransfer.TargetDocument;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import lombok.SneakyThrows;

public class LabelTransferCC15 {
	public static void main(String[] args) {
		Path baseDir = Paths.get("src/main/resources/artificial-qrels");
		
		Map<String, List<TargetDocument>> urlDuplicates = cw09OrCw12DocToCC15NearDuplicates();
		Map<String, List<TargetDocument>> simHashNearDuplicates = simHashNearDuplicates();
		List<Map<String, List<TargetDocument>>> cc15Target = Arrays.asList(urlDuplicates, simHashNearDuplicates);
		
		LabelTransfer.writeRelevanceTransferQrels(TrecSharedTask.WEB_2009, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.1-50.jsonl"), cc15Target);
		LabelTransfer.writeRelevanceTransferQrels(TrecSharedTask.WEB_2010, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.51-100.jsonl"), cc15Target);
		LabelTransfer.writeRelevanceTransferQrels(TrecSharedTask.WEB_2011, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.101-150.jsonl"), cc15Target);
		LabelTransfer.writeRelevanceTransferQrels(TrecSharedTask.WEB_2012, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.151-200.jsonl"), cc15Target);
		LabelTransfer.writeRelevanceTransferQrels(TrecSharedTask.WEB_2013, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.201-250.jsonl"), cc15Target);
		LabelTransfer.writeRelevanceTransferQrels(TrecSharedTask.WEB_2014, baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.251-300.jsonl"), cc15Target);
	}
	
	@SneakyThrows
	private static Map<String, List<TargetDocument>> cw09OrCw12DocToCC15NearDuplicates() {
		Map<String, List<TargetDocument>> ret = new LinkedHashMap<>();
		try (Stream<String> lines = Files.lines(new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/url-transfer-from-cw09-or-cw12-to-cc15-with-similarity.jsonl").toPath(), StandardCharsets.UTF_8)) {
			
			lines.forEach(line -> {
				JSONObject json = new JSONObject(line);
				String sourceId = Evaluation.cw09OrCw12(line);
				String targetId = Evaluation.cc15Id(line);
				double s3Score = json.getDouble("s3Score");
				
				if(LabelTransfer.S3_THRESHOLD <= s3Score) {
					if(!ret.containsKey(sourceId)) {
						ret.put(sourceId, new ArrayList<>());
					}
					
					ret.get(sourceId).add(new TargetDocument(targetId, s3Score));
				}
			});
		}
		
		return ret;
	}
	
	@SneakyThrows
	private static Map<String, List<TargetDocument>> simHashNearDuplicates() {
		System.out.println("Start simHashNearDuplicates :" + new Date());
		Map<String, List<TargetDocument>> ret = new LinkedHashMap<>();
		Stream<String> lines = Files.lines(new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-09-10-2020/relevance-transfer-only-near-duplicates.jsonl").toPath(), StandardCharsets.UTF_8);
		
		lines.forEach(line -> {
			String sourceId = Evaluation.cw09OrCw12(line);
			String targetId = Evaluation.cc15Id(line);
			
			if(sourceId != null && targetId != null) {
				sourceId = CanonicalDocuments.canonicalDocument(sourceId);
				
				Double s3Score = Evaluation.s3Score(line);
				if(LabelTransfer.S3_THRESHOLD <= s3Score) {
					if(!ret.containsKey(sourceId)) {
						ret.put(sourceId, new ArrayList<>());
					}
					ret.get(sourceId).add(new TargetDocument(targetId, s3Score));
				}
			}
		});
		
		lines = Files.lines(new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-09-10-2020/relevance-transfer-exact-duplicates.jsonl").toPath(), StandardCharsets.UTF_8);
		
		lines.forEach(line -> {
			String sourceId = Evaluation.cw09OrCw12(line);
			String targetId = Evaluation.cc15Id(line);
			
			if(sourceId != null && targetId != null) {
				sourceId = CanonicalDocuments.canonicalDocument(sourceId);
				
				Double s3Score = Evaluation.s3Score(line);
				if(LabelTransfer.S3_THRESHOLD <= s3Score) {
					if(!ret.containsKey(sourceId)) {
						ret.put(sourceId, new ArrayList<>());
					}
					ret.get(sourceId).add(new TargetDocument(targetId, s3Score));
				}
			}
		});
		
		System.out.println("Finish simHashNearDuplicates :" + new Date());

		return ret;
	}
}
