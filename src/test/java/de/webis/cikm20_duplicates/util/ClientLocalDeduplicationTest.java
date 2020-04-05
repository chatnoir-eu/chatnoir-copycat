package de.webis.cikm20_duplicates.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.aitools.ir.fingerprinting.representer.Hash;
import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates.DeduplicationUnit;
import scala.Tuple2;

public class ClientLocalDeduplicationTest {
	
	private static List<Integer>
		//257
		ONES = HashTransformationUtil.hashToIntegers(new byte[] {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, 3),

		//257
		ONES_THEN_SINGLE_TWO = HashTransformationUtil.hashToIntegers(new byte[] {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x02}, 3),
		
		//257
		ONES_THEN_SINGLE_THREE = HashTransformationUtil.hashToIntegers(new byte[] {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x03}, 3),

		//257
		ONES_THEN_FIVE_THREE = HashTransformationUtil.hashToIntegers(new byte[] {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x05, 0x03}, 3),

		//257
		ONES_THEN_SIX_FIVE_THREE = HashTransformationUtil.hashToIntegers(new byte[] {0x01, 0x01, 0x01, 0x01, 0x01, 0x06, 0x05, 0x03}, 3),
		//514
		TWOS = HashTransformationUtil.hashToIntegers(new byte[] {0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02}, 3);
	
	@Test
	public void checkEmitPairsForEmptyList() {
		Iterable<Tuple2<Integer, DeduplicationUnit>> group = Arrays.asList();
		List<String> expected = Collections.emptyList();
		List<String> actual = dedup(group);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void checkExceptionIsThrownForCandidatesOfDifferentGroups() {
		Iterable<Tuple2<Integer, DeduplicationUnit>> group = Arrays.asList(
			tuple(257, "1", ONES),
			tuple(514, "2", TWOS)
		);
		
		dedup(group);
	}
	
	@Test
	public void checkDeduplicationWithOnlyExactDuplicates() {
		Iterable<Tuple2<Integer, DeduplicationUnit>> group = Arrays.asList(
			tuple(257, "1", ONES),
			tuple(257, "2", ONES),
			tuple(257, "3", ONES),
			tuple(257, "4", ONES)
		);
		List<String> expected = Arrays.asList(
			nearDuplicate("1", "2", 0),
			nearDuplicate("1", "3", 0),
			nearDuplicate("1", "4", 0)
		);
		List<String> actual = dedup(group);
		
		Assert.assertEquals(expected, actual);
		
		dedup(group);
	}
	
	@Test
	public void checkDeduplicationWithOnlyExactDuplicates2() {
		Iterable<Tuple2<Integer, DeduplicationUnit>> group = Arrays.asList(
			tuple(257, "2", ONES),
			tuple(257, "1", ONES),
			tuple(257, "4", ONES),
			tuple(257, "3", ONES)
		);
		List<String> expected = Arrays.asList(
			nearDuplicate("1", "2", 0),
			nearDuplicate("1", "3", 0),
			nearDuplicate("1", "4", 0)
		);
		List<String> actual = dedup(group);
		
		Assert.assertEquals(expected, actual);
		
		dedup(group);
	}
	
	@Test
	public void checkDeduplicationWithSomeExactAndSomeNearDuplciates() {
		Iterable<Tuple2<Integer, DeduplicationUnit>> group = Arrays.asList(
			tuple(257, "2", ONES),
			tuple(257, "1", ONES),
			tuple(257, "4", ONES),
			tuple(257, "3", ONES),
			
			tuple(257, "5", ONES_THEN_SINGLE_TWO),
			tuple(257, "6", ONES_THEN_SINGLE_TWO),
			
			tuple(257, "7", Arrays.asList(257, 100, 23, 16)),
			tuple(257, "8", Arrays.asList(257, 21, 41, 18)),
			tuple(257, "9", Arrays.asList(257, 31, 97, 255))
		);
		List<String> expected = Arrays.asList(
			nearDuplicate("1", "2", 0),
			nearDuplicate("1", "3", 0),
			nearDuplicate("1", "4", 0),

			nearDuplicate("5", "6", 0),
			
			nearDuplicate("1", "5", 2)
		);
		List<String> actual = dedup(group);
		
		Assert.assertEquals(expected, actual);
		
		dedup(group);
	}
	
	@Test
	public void checkDeduplicationWithSomeDuplicatesAndMoreNearDuplicates() {
		Iterable<Tuple2<Integer, DeduplicationUnit>> group = Arrays.asList(
			tuple(257, "2", ONES_THEN_SINGLE_TWO),
			tuple(257, "1", ONES_THEN_SINGLE_TWO),
			tuple(257, "3", ONES_THEN_SINGLE_TWO),
			
			tuple(257, "5", ONES),
			tuple(257, "6", ONES),
			
			tuple(257, "7", Arrays.asList(257, 100, 23, 16)),
			tuple(257, "8", Arrays.asList(257, 21, 41, 18)),
			tuple(257, "9", Arrays.asList(257, 31, 97, 255)),
			
			tuple(257, "10", ONES_THEN_SINGLE_THREE),
			tuple(257, "11", ONES_THEN_SINGLE_THREE),
			
			tuple(257, "12", ONES_THEN_FIVE_THREE),
			
			tuple(257, "999", ONES_THEN_SIX_FIVE_THREE)
		);
		List<String> expected = Arrays.asList(
			nearDuplicate("1", "2", 0),
			nearDuplicate("1", "3", 0),

			nearDuplicate("10", "11", 0),
			nearDuplicate("5", "6", 0),

			nearDuplicate("1", "10", 1),
			nearDuplicate("1", "12", 2),
			nearDuplicate("1", "5", 2),

			nearDuplicate("10", "12", 1),
			nearDuplicate("10", "5", 1),
			
			nearDuplicate("12", "5", 2),

			nearDuplicate("12", "999", 3)
		);
		List<String> actual = dedup(group);
		
		Assert.assertEquals(expected, actual);
		
		dedup(group);
	}
	
	public static void main(String[] args) {
		System.out.println(RemovePartFromHashTest.binary(ONES_THEN_SIX_FIVE_THREE));
		System.out.println(RemovePartFromHashTest.binary(ONES_THEN_FIVE_THREE));
		
		System.out.println(Hash.getHammingDistance(
				HashTransformationUtil.integersToHash(ONES_THEN_SIX_FIVE_THREE),
				HashTransformationUtil.integersToHash(ONES_THEN_FIVE_THREE)
		));
	}
	
	private static String nearDuplicate(String id1, String id2, int hemming) {
		return "{\"firstId\":\"" + id1 + "\",\"secondId\":\"" + id2 + "\",\"hemmingDistance\":"+ hemming +"}";
	}

	private static List<String> dedup(Iterable<Tuple2<Integer, DeduplicationUnit>> group) {
		return ClientLocalDeduplication.dedup(group);
	}
	
	private static Tuple2<Integer, DeduplicationUnit> tuple(int group, String id, List<Integer> list) {
		return new Tuple2<>(group, new DeduplicationUnit(id, list));
	}
}
