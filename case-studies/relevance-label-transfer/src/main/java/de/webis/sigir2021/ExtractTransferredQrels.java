package de.webis.sigir2021;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import de.webis.sigir2021.trec.TransferredSharedTasks;
import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;
import de.webis.trec_ndd.trec_collections.SharedTask;
import lombok.SneakyThrows;

public class ExtractTransferredQrels {

	public static void main(String[] args) {
		writeTransferredQrels(TransferredSharedTasks.WEB_2009_DUPLICATE_FREE, "cw12");
		writeTransferredQrels(TransferredSharedTasks.WEB_2010_DUPLICATE_FREE, "cw12");
		writeTransferredQrels(TransferredSharedTasks.WEB_2011_DUPLICATE_FREE, "cw12");
		writeTransferredQrels(TransferredSharedTasks.WEB_2012_DUPLICATE_FREE, "cw12");
		
		writeTransferredQrels(TransferredSharedTasks.WEB_2009_DUPLICATE_FREE, "cw12wb12");
		writeTransferredQrels(TransferredSharedTasks.WEB_2010_DUPLICATE_FREE, "cw12wb12");
		writeTransferredQrels(TransferredSharedTasks.WEB_2011_DUPLICATE_FREE, "cw12wb12");
		writeTransferredQrels(TransferredSharedTasks.WEB_2012_DUPLICATE_FREE, "cw12wb12");
		
		writeTransferredQrels(TransferredSharedTasks.WEB_2009_DUPLICATE_FREE, "cc15");
		writeTransferredQrels(TransferredSharedTasks.WEB_2010_DUPLICATE_FREE, "cc15");
		writeTransferredQrels(TransferredSharedTasks.WEB_2011_DUPLICATE_FREE, "cc15");
		writeTransferredQrels(TransferredSharedTasks.WEB_2012_DUPLICATE_FREE, "cc15");
		writeTransferredQrels(TransferredSharedTasks.WEB_2013_DUPLICATE_FREE, "cc15");
		writeTransferredQrels(TransferredSharedTasks.WEB_2014_DUPLICATE_FREE, "cc15");
	}
	
	@SneakyThrows
	public static void writeTransferredQrels(SharedTask task, String transferTo) {
		Path targetPath = targetPath(task, transferTo);
		String fileContent = extractTransferredQrels(task, transferTo);
		
		Files.write(targetPath, fileContent.getBytes());
	}
	
	@SneakyThrows
	public static String extractTransferredQrels(SharedTask task, String transferTo) {
		List<String> inputLines = IOUtils.readLines(ExtractTransferredQrels.class.getResourceAsStream(inputPathOrFail(task, transferTo).toString()));
		
		return inputLines.stream().filter(i -> hasRelevanceTransfer(i))
				.map(i -> toQrel(i))
				.collect(Collectors.joining("\n"));
	}
	
	private static String toQrel(String rawJson) {
		JSONObject json = new JSONObject(rawJson);
		QrelEqualWithoutScore ret = new QrelEqualWithoutScore();
		ret.setScore(json.getInt("relevanceLabel"));
		ret.setDocumentID(json.getString("sourceDocument"));
		ret.setTopicNumber(json.getInt("topicNumber"));
		
		return ret.toString();
	}

	private static boolean hasRelevanceTransfer(String rawJson) {
		JSONObject json = new JSONObject(rawJson);

		return json.has("targetDocs") && json.getJSONArray("targetDocs").length() > 0;
	}

	private static Path inputPathOrFail(SharedTask task, String transferTo) {
		if(task == null || transferTo == null || task.getQrelResource() == null || !task.getQrelResource().contains(".web.")) {
			throw new IllegalArgumentException();
		}
		
		String ret = StringUtils.replace(task.getQrelResource(), ".web.", ".transferred-to-" + transferTo +".");
		return Paths.get(StringUtils.replace(ret, ".txt", ".jsonl"));
	}
	
	public static Path targetPath(SharedTask task, String transferTo) {
		String ret = StringUtils.replace(task.getQrelResource(), ".web.", ".transferred-to-" + transferTo +".");
		ret = StringUtils.replace(ret, "/artificial-qrels/", "src/main/resources/artificial-qrels/qrels-with-only-transferred-original-labels/");
		
		return Paths.get(ret);
	}
}
