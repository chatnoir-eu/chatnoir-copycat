package de.webis.sigir2021;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.sigir2021.trec.JudgedDocuments;
import de.webis.sigir2021.trec.TransferredSharedTasks;
import de.webis.trec_ndd.trec_collections.SharedTask;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;

public class CreateOutputForRankingExperiments {

	public static void main(String[] args) {
		Path baseDir = Paths.get("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/ranking-tasks");
		
		//md5sum 3bf55a91362e3fe51c161f6cf2c7f4a3
		writeRankingTasks(TransferredSharedTasks.WEB_2009_DUPLICATE_FREE, baseDir.resolve("topics-1-50-cw09.jsonl"));
		
		//md5sum abac778ee81110f8f540aaa70b1941d2
		writeRankingTasks(TransferredSharedTasks.WEB_2010_DUPLICATE_FREE, baseDir.resolve("topics-51-100-cw09.jsonl"));
		
		//md5sum 33248beb3ff5325410202d2a10e92263
		writeRankingTasks(TransferredSharedTasks.WEB_2011_DUPLICATE_FREE, baseDir.resolve("topics-101-150-cw09.jsonl"));
		
		//md5sum 68eef2a58f8cb23c4af999d684664b06
		writeRankingTasks(TransferredSharedTasks.WEB_2012_DUPLICATE_FREE, baseDir.resolve("topics-151-200-cw09.jsonl"));
		
		//md5sum b1885d716c761f7835d17fae218e9886
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012, baseDir.resolve("topics-1-50-cw12.jsonl"));
		
		//md5sum d2b4d05ad0cbb945238da2844957a596
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_2012, baseDir.resolve("topics-51-100-cw12.jsonl"));
		
		//md5sum 3266c991f2452df386f37cdad62c2810
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_2012, baseDir.resolve("topics-101-150-cw12.jsonl"));
		
		//md5sum e945c3908f4a749da1ddf1428b2188ab
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_2012, baseDir.resolve("topics-151-200-cw12.jsonl"));
		
		//md5sum afc96fc8e39c5dc977dbf3095d104281
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012, baseDir.resolve("topics-1-50-cw12wb12.jsonl"));
		
		//md5sum 2f296805d162205739a33679887100c1
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_WB_2012, baseDir.resolve("topics-51-100-cw12wb12.jsonl"));
		
		//md5sum 7fe7306da65fc7ce62dd10a99cdfe751
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_WB_2012, baseDir.resolve("topics-101-150-cw12wb12.jsonl"));
		
		//md5sum 57d62496b245e92a4cfea75383ee118e
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_WB_2012, baseDir.resolve("topics-151-200-cw12wb12.jsonl"));
		
		//md5sum 56acd46bb60fe8cab1ebb44247ab0846
		writeRankingTasks(TransferredSharedTasks.WEB_2013_DUPLICATE_FREE, baseDir.resolve("topics-201-250-cw12.jsonl"));
		
		//md5sum 84b40e447efd3e0f23b719fa417715ab
		writeRankingTasks(TransferredSharedTasks.WEB_2014_DUPLICATE_FREE, baseDir.resolve("topics-251-300-cw12.jsonl"));
		
		//md5sum 11f89eb2bd45c2dc7b65eb61753ed37a
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CC_2015, baseDir.resolve("topics-1-50-cc15.jsonl"));
		
		//md5sum ae6f1a973422e1991bf448d071bbe8b8
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CC_2015, baseDir.resolve("topics-51-100-cc15.jsonl"));
		
		//md5sum 8a1c05b7bbdb5293732e01cfb942b5c7
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CC_2015, baseDir.resolve("topics-101-150-cc15.jsonl"));
		
		//md5sum 0d510477730a3a2b1322df48d5a5627f
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CC_2015, baseDir.resolve("topics-151-200-cc15.jsonl"));
		
		//md5sum 376912e809c784a54ed4d65b0613636e
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2013_TO_CC_2015, baseDir.resolve("topics-201-250-cc15.jsonl"));
		
		//md5sum 4886bcd7e4b00ad74cd58ca73d1d01b5
		writeRankingTasks(TransferredSharedTasks.TRANSFER_FROM_WEB_2014_TO_CC_2015, baseDir.resolve("topics-251-300-cc15.jsonl"));
	}
	
	@SneakyThrows
	private static void writeRankingTasks(SharedTask task, Path path) {
		List<String> ret = new ArrayList<>();
		
		for(String topic: task.topicNumberToTopic().keySet()) {
			RankingTask t = new RankingTask(
				Integer.parseInt(topic),
				task.topicNumberToTopic().get(topic).get("title"),
				documentsToScore(task, topic)
			);
			System.out.println(t);
			ret.add(t.toString());
		}
		
		Files.write(path, ret.stream().collect(Collectors.joining("\n")).getBytes());
	}
	
	private static List<CNDocument> documentsToScore(SharedTask task, String topic) {
		List<CNDocument> ret = new ArrayList<>();
		
		for(String relevantDoc: task.documentJudgments().getRelevantDocuments(topic)) {
			ret.add(new CNDocument(relevantDoc, task));
		}
		
		for(String irrelevantDoc: task.documentJudgments().getIrrelevantDocuments(topic)) {
			ret.add(new CNDocument(irrelevantDoc, task));
		}
		
		return ret;
	}
	
	@Data
	@AllArgsConstructor
	public static class RankingTask {
		private int topicNumber;
		private String query;
		private List<CNDocument> documentsToScore;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
	}

	@Data
	public static class CNDocument {
		private String index;
		private String recordType;
		private String trecId;
		private String chatNoirId;
		
		public CNDocument(String id, SharedTask task) {
			if(isCw09OrCw12Task(task)) {
				this.index = JudgedDocuments.index(id);
				this.recordType = JudgedDocuments.type(id);
				this.trecId = id;
				this.chatNoirId = JudgedDocuments.chatNoirId(id);
			} else if(isCW12OrWaybackTask(task)) {
				if(id.startsWith("clueweb")) {
					this.chatNoirId = JudgedDocuments.chatNoirId(id);
				} else if(id.startsWith("<urn:")) {
					this.chatNoirId = JudgedDocuments.chatNoirId("clueweb09-in-wayback12", id).toString();
				} else {
					throw new IllegalArgumentException("Could not handle id " + id);
				}
				
				this.index = "clueweb12-and-wayback12";
				this.recordType = JudgedDocuments.type(id);
				this.trecId = id;
			} else if(isCC15Task(task)) {
				this.index = "webis_warc_commoncrawl15_002";
				this.recordType = JudgedDocuments.type(id);
				this.trecId = id;
				this.chatNoirId = JudgedDocuments.chatNoirId("commoncrawl", id).toString();
			} else {
				throw new IllegalArgumentException("Could not handle task " + task);
			}
		}
		
		private static boolean isCw09OrCw12Task(SharedTask task) {
			return task != null && task.getQrelResource() != null &&
					Arrays.asList("/artificial-qrels/qrels.inofficial.duplicate-free.web.1-50.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.web.51-100.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.web.101-150.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.web.151-200.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.1-50.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.51-100.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.101-150.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.151-200.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.web.201-250.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.web.251-300.txt").contains(task.getQrelResource());
		}
		
		private static boolean isCW12OrWaybackTask(SharedTask task ) {
			return task != null && task.getQrelResource() != null &&
					Arrays.asList("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12wb12.1-50.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12wb12.51-100.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12wb12.101-150.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12wb12.151-200.txt").contains(task.getQrelResource());
		}
		
		private static boolean isCC15Task(SharedTask task) {
			return task != null && task.getQrelResource() != null &&
					Arrays.asList("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.1-50.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.51-100.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.101-150.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.151-200.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.201-250.txt",
							"/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.251-300.txt").contains(task.getQrelResource());
		}
	}
}
