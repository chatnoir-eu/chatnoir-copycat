package de.webis.cikm20_duplicates.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import scala.Tuple3;

public class ExtractPairsWithJudgedDocumentsTest {
	@Test
	public void checkExtractedPairsForOnlyUnjudgedEquivalenceClass() {
		List<String> ids = Arrays.asList("clueweb09-2", "clueweb09-3", "clueweb09-5", "clueweb09-1");
		List<Tuple3<String, String, Integer>> expected = Collections.emptyList();
		List<Tuple3<String, String, Integer>> actual = extractAllPairsWithJudgedDocuments(ids);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkExtractedPairsForOneEquivalenceClassWithSingleRelevantDocument() {
		List<String> ids = Arrays.asList("clueweb09-2", "clueweb09-3", "clueweb09-en0008-02-29970", "clueweb09-1");
		List<Tuple3<String, String, Integer>> expected = Arrays.asList(
			new Tuple3<>("clueweb09-1", "clueweb09-en0008-02-29970", 0),
			new Tuple3<>("clueweb09-2", "clueweb09-en0008-02-29970", 0),
			new Tuple3<>("clueweb09-3", "clueweb09-en0008-02-29970", 0)
		);
		List<Tuple3<String, String, Integer>> actual = extractAllPairsWithJudgedDocuments(ids);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkExtractedPairsForOneEquivalenceClassWithSingleRelevantDocument2() {
		List<String> ids = Arrays.asList("clueweb09-2", "clueweb09-3", "clueweb09-en0008-02-29970", "clueweb12-1800tw-04-16339", "clueweb09-1");
		List<Tuple3<String, String, Integer>> expected = Arrays.asList(
			new Tuple3<>("clueweb09-1", "clueweb09-en0008-02-29970", 0),
			new Tuple3<>("clueweb09-2", "clueweb09-en0008-02-29970", 0),
			new Tuple3<>("clueweb09-3", "clueweb09-en0008-02-29970", 0),
			new Tuple3<>("clueweb09-en0008-02-29970", "clueweb12-1800tw-04-16339", 0)
		);
		List<Tuple3<String, String, Integer>> actual = extractAllPairsWithJudgedDocuments(ids);
		
		Assert.assertEquals(expected, actual);
	}
	
	private static List<Tuple3<String, String, Integer>> extractAllPairsWithJudgedDocuments(List<String> ids) {
		return ImmutableList.copyOf(SparkCountEdgeLabels.extractAllPairsWithJudgedDocuments(ids))
				.stream().sorted((i,j) -> i.toString().compareTo(j.toString()))
				.collect(Collectors.toList());
	}
}
