package de.webis.cikm20_duplicates.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.curator.shaded.com.google.common.collect.Iterators;

import com.google.common.collect.ImmutableList;

import de.aitools.ir.fingerprinting.representer.Hash;
import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates.DeduplicationUnit;
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
		
		for(List<Tuple2<Integer, DeduplicationUnit>> equal: equals.values()) {
			if(equal.size() < 1) {
				continue;
			}
			
			Tuple2<Integer, DeduplicationUnit> i = equal.get(0);
			List<Integer> rekursiveInts = HashTransformationUtil.removeIntFromUnderlyingBitArray(i._2.getHashParts(), i._1());
			for(int recursivePart: rekursiveInts) {
				if(!ret.containsKey(recursivePart)) {
					ret.put(recursivePart, new LinkedList<>());
				}
				
				ret.get(recursivePart).add(i._2);
			}
		}
		
		return ret.values();
	}
	
	private static List<String> fullDeduplication(List<DeduplicationUnit> l) {
		List<String> ret = new LinkedList<>();
		
		for(int i=0; i<l.size(); i++) {
			for(int j=i+1; j< l.size(); j++) {
				int hemming = hemming(l.get(i), l.get(j));
				if(hemming <= 3) {
					ret.add(nearDuplicate(l.get(i).getId(), l.get(j).getId(), hemming));
				}
			}
		}
		
		return ret;
	}
	
	private static int hemming(DeduplicationUnit a, DeduplicationUnit b) {
		byte[] aArr = HashTransformationUtil.integersToHash(a.getHashParts());
		byte[] bArr = HashTransformationUtil.integersToHash(b.getHashParts());
		
		return Hash.getHammingDistance(aArr, bArr);
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
}
