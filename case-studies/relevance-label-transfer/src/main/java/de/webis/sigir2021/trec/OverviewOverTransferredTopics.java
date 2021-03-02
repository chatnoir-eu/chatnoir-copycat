package de.webis.sigir2021.trec;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;
import de.webis.trec_ndd.trec_collections.SharedTask;
import lombok.SneakyThrows;

public class OverviewOverTransferredTopics {

	@SneakyThrows
	public static void main(String[] args) {
		List<String> ret = new ArrayList<>();
	
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2009_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012
		));
		
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2010_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_2012
		));
		
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2011_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_2012
		));
		
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2012_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_2012
		));
		
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2009_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012
		));
			
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2010_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_WB_2012
		));
			
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2011_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_WB_2012
		));
			
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2012_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_WB_2012
		));
		
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2009_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CC_2015
		));
	
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2010_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CC_2015
		));
		
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2011_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CC_2015
		));
		
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2012_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CC_2015
		));
		
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2013_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2013_TO_CC_2015
		));
		
		ret.addAll(overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2014_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2014_TO_CC_2015
		));
		
		Files.write(Paths.get("src/test/resources/overview-of-transferred-topics.jsonl"), ret.stream().collect(Collectors.joining("\n")).getBytes());
	}
	
	public static List<String> overviewOverTransferredTopics(SharedTask original, SharedTask transferred) {
		TransferredTopicSelectionStrategies.failWhenArgumentsAreInWrongOrder(original.getQrelResourcesWithoutScore(), transferred.getQrelResourcesWithoutScore());
		
		return overviewOverTransferredTopics(transferred.getQrelResourcesWithoutScore(), TransferredTopicSelectionStrategies.targetCorpus(transferred));
	}

	private static List<String> overviewOverTransferredTopics(Set<QrelEqualWithoutScore> qrels, String corpus) {
		List<String> ret = new ArrayList<>();
		
		for(int topic: TransferredTopicSelectionStrategies.topics(qrels)) {
			ret.add(entry(topic, qrels, corpus));
		}

		return ret;
	}
	
	@SneakyThrows
	private static String entry(int topic, Set<QrelEqualWithoutScore> qrels, String corpus) {
		int relevant = 0;
		int irrelevant = 0;
		for(QrelEqualWithoutScore qrel: qrels) {
			if(qrel.getTopicNumber() == topic) {
				if(qrel.getScore() > 0) {
					relevant++;
				} else {
					irrelevant++;
				}
			}
		}
		
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("topic", "" + topic);
		ret.put("targetCorpus", corpus);
		ret.put("relevant", relevant);
		ret.put("irrelevant", irrelevant);
		
		return new ObjectMapper().writeValueAsString(ret);
	}
}
