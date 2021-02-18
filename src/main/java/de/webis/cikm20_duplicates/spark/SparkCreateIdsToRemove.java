package de.webis.cikm20_duplicates.spark;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

public class SparkCreateIdsToRemove {
	
	@SuppressWarnings("serial")
	public static KeepId 
		CLUEWEB09 = new KeepId() {
			@Override
			public boolean keepId(String id) {
				return id != null && id.startsWith("clueweb09");
			}
		},
		
		CLUEWEB12 = new KeepId() {
			@Override
			public boolean keepId(String id) {
				return id != null && id.startsWith("clueweb12");
			}
		},
		
		COMMON_CRAWL = new KeepId() {
			@Override
			public boolean keepId(String id) {
				return id != null && !id.startsWith("clueweb12") && !id.startsWith("clueweb09");
			}
		},
		
		ALL_CRAWLS = new KeepId() {
			@Override
			public boolean keepId(String id) {
				return true;
			}
		};

	private static List<String> docsToRemoveFromNearDuplicates(String src, KeepId keepId) {
		try {
			return docsToRemoveFromNearDuplicatesInJsonFormat(src, keepId);
		} catch(Exception e) {
			if(StringUtils.countMatches(src, ',') == 1) {
				return docsToRemoveFromNearDuplicatesInCsvFormat(src, keepId);
			}
			
			throw new RuntimeException(e);
		}
	}		

	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static List<String> docsToRemoveFromNearDuplicatesInJsonFormat(String src, KeepId keepId) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		String firstId = (String) parsed.get("firstId");
		String secondId = (String) parsed.get("secondId");
		
		return idsToRemove(Arrays.asList(firstId, secondId), keepId);
	}
	
	private static List<String> docsToRemoveFromNearDuplicatesInCsvFormat(String src, KeepId keepId) {
		String firstId = StringUtils.substringBefore(src, ",");
		String secondId = StringUtils.substringAfter(src, ",");
		
		return idsToRemove(Arrays.asList(firstId, secondId), keepId);
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static List<String> docsToRemoveFromExactDuplicates(String src, KeepId keepId) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		List<String> ids = (List<String>) parsed.get("equivalentDocuments");
		
		return idsToRemove(ids, keepId);
	}

	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static int idsInExactDuplicates(String src, KeepId keepId) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		List<String> ids = (List<String>) parsed.get("equivalentDocuments");
		ids = ids.stream().filter(id -> keepId.keepId(id)).collect(Collectors.toList());
		
		if(ids == null || ids.isEmpty()) {
			return 0;
		} else {
			return ids.size();
		}
	}
	
	public static JavaRDD<String> idsToRemoveNonDistinct(JavaRDD<String> nearDuplicates, JavaRDD<String> exactDuplicates, KeepId keepId) {
		nearDuplicates = nearDuplicates.flatMap(i -> docsToRemoveFromNearDuplicates(i, keepId).iterator())
				.filter(i -> i != null);
		exactDuplicates = exactDuplicates.flatMap(i -> docsToRemoveFromExactDuplicates(i, keepId).iterator())
				.filter(i -> i != null);
		
		return nearDuplicates.union(exactDuplicates);
	}
	
	public static JavaRDD<String> idsToRemove(JavaRDD<String> nearDuplicates, JavaRDD<String> exactDuplicates, KeepId keepId) {
		return idsToRemoveNonDistinct(nearDuplicates, exactDuplicates, keepId)
				.distinct();
	}

	public static List<String> idsToRemove(List<String> ids, KeepId keepId) {
		if(ids == null || ids.size() < 2) {
			return Collections.emptyList();
		}
		
		ids = new ArrayList<>(new HashSet<>(ids).stream()
				.filter(i -> keepId.keepId(i))
				.collect(Collectors.toList()));
		if(ids.size() < 2) {
			return Collections.emptyList();
		}
		
		Collections.sort(ids);
		ids.remove(0);
		
		return ids;
	}
	
	@SneakyThrows
	@SuppressWarnings("serial")
	public static KeepId idsToKeepFromFile(String file) {
		Set<String> idsToKeep = new HashSet<>(Files.readAllLines(Paths.get(file)));
		
		return new KeepId() {
			@Override
			public boolean keepId(String id) {
				return idsToKeep.contains(id);
			}
		};
	}
	
	public static interface KeepId extends Serializable {
		public boolean keepId(String id);
	}
}
