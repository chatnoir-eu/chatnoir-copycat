package de.webis.cikm20_duplicates.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.curator.shaded.com.google.common.collect.Iterators;
import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.ImmutableList;

import de.aitools.ir.fingerprinting.representer.Hash;
import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates.DeduplicationUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import scala.Tuple2;

public class ClientLocalDeduplication {

	public static List<String> dedup(Iterable<Tuple2<Integer, DeduplicationUnit>> group) {
		List<String> ret = new ArrayList<>();
		Map<String, List<Tuple2<Integer, DeduplicationUnit>>> equals = sortedList(group);

		for(List<Tuple2<Integer, DeduplicationUnit>> equal: equals.values()) {
			ret.addAll(equalPairs(equal));
		}
		
		for(List<DeduplicationUnit> toDedup: deduplicationPairs(equals)) {
			for(String bla : fullDeduplication(toDedup)) {
				if(!ret.contains(bla)) {
					ret.add(bla);
				}
			}
		}
		
		return ret;
	}
	
	public static Collection<List<DeduplicationUnit>> deduplicationPairs(Map<String, List<Tuple2<Integer, DeduplicationUnit>>> equals) {
		Map<Integer, List<DeduplicationUnit>> ret = new LinkedHashMap<>();
		
		if(equals.size() < 100000) {
			List<DeduplicationUnit> tmpRet = new ArrayList<>();
			for(List<Tuple2<Integer, DeduplicationUnit>> equal: equals.values()) {
				if(equal.size() < 1) {
					continue;
				}
				
				tmpRet.add(equal.get(0)._2);
			}
			
			return Arrays.asList(tmpRet);
		}
		
		for(List<Tuple2<Integer, DeduplicationUnit>> equal: equals.values()) {
			if(equal.size() < 1) {
				continue;
			}
			
			Tuple2<Integer, DeduplicationUnit> i = equal.get(0);
			List<Integer> rekursiveInts = HashTransformationUtil.removeIntFromUnderlyingBitArray(i._2.getHashParts(), i._1());
			for(int recursivePart: rekursiveInts) {
				if(!ret.containsKey(recursivePart)) {
					ret.put(recursivePart, new ArrayList<>());
				}
				
				ret.get(recursivePart).add(i._2);
			}
		}
		
		return ret.values();
	}
	
	public static List<String> fullDeduplication(List<DeduplicationUnit> orig) {
		List<String> ret = new LinkedList<>();
		List<Tuple2<String, byte[]>> idToHash = new ArrayList<>(orig.size());
		
		for(DeduplicationUnit d: orig) {
			idToHash.add(new Tuple2<>(d.getId(), HashTransformationUtil.integersToHash(d.getHashParts())));
		}
		
		for(int i=0; i<idToHash.size(); i++) {
			Tuple2<String, byte[]> left = idToHash.get(i);

			for(int j=i+1; j< idToHash.size(); j++) {
				Tuple2<String, byte[]> right = idToHash.get(j);
				
				int hemming = Hash.getHammingDistance(left._2(), right._2());
				if(hemming <= 3) {
					ret.add(nearDuplicate(left._1(), right._1(), hemming));
				}
			}
		}
		
		return ret;
	}

	private static Collection<String> equalPairs(List<Tuple2<Integer, DeduplicationUnit>> equal) {
		List<String> ret = new ArrayList<>();
		
		for(int i= 1; i< equal.size(); i++) {
			ret.add(nearDuplicate(equal.get(0)._2.getId(), equal.get(i)._2.getId(), 0));
		}
		
		return ret;
	}
	
	private static String nearDuplicate(String id1, String id2, int hemming) {
		return "{\"firstId\":\"" + id1 + "\",\"secondId\":\"" + id2 + "\",\"hemmingDistance\":"+ hemming +"}";
	}


	private static void failIfElementsAreNotAllFromTheSameGroup(Iterable<Tuple2<Integer, DeduplicationUnit>> group) {
		Set<Integer> ret = new HashSet<>();
		Iterators.transform(group.iterator(), i -> i._1()).forEachRemaining(i -> ret.add(i));
		
		if(ret.size() > 1) {
			throw new IllegalArgumentException("");
		}
	}
	
	public static Map<String, List<Tuple2<Integer, DeduplicationUnit>>> sortedList(Iterable<Tuple2<Integer, DeduplicationUnit>> group) {
		List<Tuple2<Integer, DeduplicationUnit>> tmp = ImmutableList.copyOf(group.iterator());
		tmp = new ArrayList<>(tmp);
		Collections.sort(tmp, (a,b) -> a._2.getId().compareTo(b._2.getId()));
		failIfElementsAreNotAllFromTheSameGroup(tmp);
		
		Map<String, List<Tuple2<Integer, DeduplicationUnit>>> ret = new LinkedHashMap<>();
		for(Tuple2<Integer, DeduplicationUnit> t : tmp) {
			String key = t._2().getHashParts().toString();
			
			if(!ret.containsKey(key)) {
				ret.put(key, new LinkedList<>());
			}
			
			ret.get(key).add(t);
		}
		
		
		return ret;
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DeduplicationTask {
		private List<DeduplicationUnit> entries;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
		
		@SneakyThrows
		public static DeduplicationTask fromString(String value) {
			return new ObjectMapper().readValue(value, DeduplicationTask.class);
		}
	}

	public static java.util.Iterator<String> workingPackages(Iterable<Tuple2<Integer, DeduplicationUnit>> group) {
		Map<String, List<Tuple2<Integer, DeduplicationUnit>>> equals = sortedList(group);
		Collection<List<DeduplicationUnit>> ret = deduplicationPairs(equals);
		
		return com.google.common.collect.Iterators.transform(
			ret.iterator(), 
			i -> new DeduplicationTask(i).toString()
		);
	}
}
