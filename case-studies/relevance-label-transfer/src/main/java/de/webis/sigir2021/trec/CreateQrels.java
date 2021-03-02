package de.webis.sigir2021.trec;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import de.webis.sigir2021.trec.LabelTransfer.RelevanceTransfer;
import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import lombok.SneakyThrows;

public class CreateQrels {
	public static void main(String[] args) {
		Path baseDir = Paths.get("src/main/resources/artificial-qrels");
		
		writeDuplicateFreeQrels(TrecSharedTask.WEB_2009, baseDir.resolve("qrels.inofficial.duplicate-free.web.1-50.txt"));
		writeDuplicateFreeQrels(TrecSharedTask.WEB_2010, baseDir.resolve("qrels.inofficial.duplicate-free.web.51-100.txt"));
		writeDuplicateFreeQrels(TrecSharedTask.WEB_2011, baseDir.resolve("qrels.inofficial.duplicate-free.web.101-150.txt"));
		writeDuplicateFreeQrels(TrecSharedTask.WEB_2012, baseDir.resolve("qrels.inofficial.duplicate-free.web.151-200.txt"));
		writeDuplicateFreeQrels(TrecSharedTask.WEB_2013, baseDir.resolve("qrels.inofficial.duplicate-free.web.201-250.txt"));
		writeDuplicateFreeQrels(TrecSharedTask.WEB_2014, baseDir.resolve("qrels.inofficial.duplicate-free.web.251-300.txt"));
		
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12.1-50.jsonl"));
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12.51-100.jsonl"));
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12.101-150.jsonl"));
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12.151-200.jsonl"));
		
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12wb12.1-50.jsonl"));
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12wb12.51-100.jsonl"));
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12wb12.101-150.jsonl"));
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cw12wb12.151-200.jsonl"));
		
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.1-50.jsonl"));
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.51-100.jsonl"));
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.101-150.jsonl"));
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.151-200.jsonl"));
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.201-250.jsonl"));
		jsonlToQrels(baseDir.resolve("qrels.inofficial.duplicate-free.transferred-to-cc15.251-300.jsonl"));
	}
	
	@SneakyThrows
	private static void jsonlToQrels(Path jsonlFile) {
		Path targetFile = jsonlFile.getParent().resolve(jsonlFile.getFileName().toString().replaceAll("\\.jsonl", ".txt"));
		List<QrelEqualWithoutScore> lines = Files.readAllLines(jsonlFile).stream().map(i -> jsonToQrel(i)).collect(Collectors.toList());
		//FIXME: Do sanity-checks
		String ret = lines.stream().filter(i -> i != null).map(i-> i.toString()).collect(Collectors.joining("\n"));
		Files.write(targetFile, ret.getBytes());
	}

	private static QrelEqualWithoutScore jsonToQrel(String json) {
		RelevanceTransfer rt = RelevanceTransfer.fromString(json);
		if(rt.getTargetDocs() == null || rt.getTargetDocs().isEmpty()) {
			return null;
		}
		
		QrelEqualWithoutScore ret = new QrelEqualWithoutScore();
		ret.setDocumentID(rt.getTargetDocs().get(0).getTargetId());
		ret.setScore(rt.getRelevanceLabel());
		ret.setTopicNumber(rt.getTopicNumber());
		
		return ret;
	}

	@SneakyThrows
	private static void writeDuplicateFreeQrels(SharedTask task, Path path) {
		String qrels = duplicateFreeQrels(task).stream().collect(Collectors.joining("\n"));
		Files.write(path, qrels.getBytes());
	}
	
	private static List<String> duplicateFreeQrels(SharedTask task) {
		List<String> ret = new ArrayList<>();
		
		for(QrelEqualWithoutScore qrel: task.getQrelResourcesWithoutScore()) {
			if(!CanonicalDocuments.isDuplicate(qrel.getDocumentID())) {
				System.out.println(qrel);
				ret.add(qrel.toString());
			}
		}
		
		return ret;
	}
}
