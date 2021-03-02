package de.webis.sigir2021.trec;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.math.BigIntegerMath;

import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;
import de.webis.trec_ndd.trec_collections.SharedTask;
import lombok.SneakyThrows;

public class TransferredTopicSelectionStrategies {

	@SneakyThrows
	public static void main(String[] args) {
		List<String> ret = new ArrayList<>();
		
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2009_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012
		));
		
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2010_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_2012
		));
		
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2011_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_2012
		));
		
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2012_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_2012
		));
		
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2009_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012
		));
			
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2010_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_WB_2012
		));
			
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2011_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_WB_2012
		));
			
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2012_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_WB_2012
		));
		
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2009_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CC_2015
		));

		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2010_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CC_2015
		));
		
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2011_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CC_2015
		));
		
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2012_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CC_2015
		));
		
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2013_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2013_TO_CC_2015
		));
		
		ret.addAll(enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2014_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2014_TO_CC_2015
		));
		
		Files.write(Paths.get("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-19-10-2020/topic-selection-features.jsonl"), ret.stream().collect(Collectors.joining("\n")).getBytes());
	}
	
	public static List<String> enrichTopicsWithSelectionCriteria(SharedTask original, SharedTask transferred) {
		System.out.println(original.getQrelResource() + " -> " + transferred.getQrelResource());
		
		Set<QrelEqualWithoutScore> targetQrels = transferred.getQrelResourcesWithoutScore();
		List<TopicFeature> features = Arrays.asList(
			new TopicFeature() {
				@Override
				public Object feature(String topic) {
					return countTopic(targetQrels, Integer.valueOf(topic));
				}

				@Override
				public String name() {
					return "labelCount";
				}
			},
			new TopicFeature() {
				@Override
				public Object feature(String topic) {
					return TransferredTopicSelectionStrategies.kPermutations(targetQrels, Integer.valueOf(topic));
				}

				@Override
				public String name() {
					return "possibleLabelPermutations";
				}
			},
			new TopicFeature() {
				@Override
				public Object feature(String topic) {
					return -1;
				}

				@Override
				public String name() {
					return "empiricalLabelPermutations";
				}
			}
		);
		
		return enrichTopics(original.getQrelResourcesWithoutScore(), targetQrels, transferred, features);
	}

	@SneakyThrows
	private static List<String> enrichTopics(Set<QrelEqualWithoutScore> original, Set<QrelEqualWithoutScore> transferred, SharedTask target, List<TopicFeature> features) {
		failWhenArgumentsAreInWrongOrder(original, transferred);
		
		List<String> topics = new ArrayList<>(topics(original).stream().map(i -> String.valueOf(i)).collect(Collectors.toList()));
		List<String> ret = new ArrayList<>();
		
		for(String topic: topics) {
			Map<String, Object> topicFeatures = new LinkedHashMap<>();
			topicFeatures.put("topic", Integer.valueOf(topic));
			topicFeatures.put("targetCorpus", targetCorpus(target));
			for(TopicFeature feature: features) {
				topicFeatures.put(feature.name(), feature.feature(topic));
			}

			ret.add(new ObjectMapper().writeValueAsString(topicFeatures));
		}
		
		return ret;
	}
	
	static String targetCorpus(SharedTask target) {
		return StringUtils.substringBetween(target.getQrelResource(), "-to-", ".");
	}

	static void failWhenArgumentsAreInWrongOrder(Set<QrelEqualWithoutScore> original, Set<QrelEqualWithoutScore> transferred) {
		int transferredLabels = 0;
		for(int topic: topics(original)) {
			transferredLabels += countTopic(transferred, topic);
			if(countTopic(original, topic) < countTopic(transferred, topic)) {
				throw new IllegalArgumentException("Fix this");
			}
		}
		
		if(transferredLabels < 50) {
			throw new IllegalArgumentException("Fix this");
		}
	}
	
	private static int countTopic(Set<QrelEqualWithoutScore> qrels, int topic) {
		return (int) qrels.stream().filter(i -> i.getTopicNumber() == topic).count();
	}
	
	static Set<Integer> topics(Set<QrelEqualWithoutScore> qrels) {
		return qrels.stream().map(i -> i.getTopicNumber()).collect(Collectors.toSet());
	}
	
	private static interface TopicFeature {
		public Object feature(String topic);
		public String name();
	}
	
	static BigInteger kPermutations(Set<QrelEqualWithoutScore> targetQrels, Integer topic) {
		targetQrels = targetQrels.stream().filter(i -> i.getTopicNumber() == topic).collect(Collectors.toSet());
		BigInteger k = BigIntegerMath.factorial(targetQrels.size());
		BigInteger denominator = BigInteger.ONE;
		Map<String, Integer> labelToCount = new HashMap<>();
		
		for(QrelEqualWithoutScore qrel: targetQrels) {
			String label = String.valueOf(qrel.getScore());
			
			if(!labelToCount.containsKey(label)) {
				labelToCount.put(label, 0);
			}
			
			labelToCount.put(label, 1+ labelToCount.get(label));
		}
		
		for(Integer count: labelToCount.values()) {
			denominator = denominator.multiply(BigIntegerMath.factorial(count));
		}
		
		return k.divide(denominator);
	}
}
