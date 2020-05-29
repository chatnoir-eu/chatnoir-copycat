package de.webis.cikm20_duplicates.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

public class SparkCreateIdsToRemove {

	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static List<String> docsToRemoveFromNearDuplicates(String src, String idPrefix) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		String firstId = (String) parsed.get("firstId");
		String secondId = (String) parsed.get("secondId");
		
		return idsToRemove(Arrays.asList(firstId, secondId), idPrefix);
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static List<String> docsToRemoveFromExactDuplicates(String src, String idPrefix) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		List<String> ids = (List<String>) parsed.get("equivalentDocuments");
		
		return idsToRemove(ids, idPrefix);
	}
	
	public static JavaRDD<String> idsToRemove(JavaRDD<String> nearDuplicates, JavaRDD<String> exactDuplicates, String idPrefix) {
		nearDuplicates = nearDuplicates.flatMap(i -> docsToRemoveFromNearDuplicates(i, idPrefix).iterator())
				.filter(i -> i != null);
		exactDuplicates = exactDuplicates.flatMap(i -> docsToRemoveFromExactDuplicates(i, idPrefix).iterator())
				.filter(i -> i != null);
		
		return nearDuplicates.union(exactDuplicates).distinct();
	}

	public static List<String> idsToRemove(List<String> ids, String idPrefix) {
		if(ids == null || ids.size() < 2) {
			return Collections.emptyList();
		}
		
		ids = new ArrayList<>(new HashSet<>(ids).stream()
				.filter(i -> i.startsWith(idPrefix))
				.collect(Collectors.toList()));
		if(ids.size() < 2) {
			return Collections.emptyList();
		}
		
		Collections.sort(ids);
		ids.remove(0);
		
		return ids;
	}
}
