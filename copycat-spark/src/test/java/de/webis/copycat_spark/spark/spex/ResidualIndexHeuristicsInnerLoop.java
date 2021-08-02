package de.webis.copycat_spark.spark.spex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.spark.SparkIntegrationTestBase;
import de.webis.copycat_spark.spark.spex.ResidualIndexHeuristics.ResidualIndexHeuristic;

public class ResidualIndexHeuristicsInnerLoop extends SparkIntegrationTestBase {
	@Test(expected=RuntimeException.class)
	public void testUnsortedListFails() {
		List<ResidualIndexHeuristic> input = Arrays.asList(
			new ResidualIndexHeuristic("id-1", 1, 1),
			new ResidualIndexHeuristic("id-2", 2, 0)
		);
		
		extractCandidates(input, 0.5);
	}
	
	@Test
	public void approveExtractionOfCandidatesForIdenticalLength() {
		List<ResidualIndexHeuristic> input = Arrays.asList(
			new ResidualIndexHeuristic("id-1", 1, 2),
			new ResidualIndexHeuristic("id-2", 1, 2)
		);
		
		List<String> actual = extractCandidates(input, 0.5);
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveExtractionOfCandidatesWithDifferentLength() {
		List<ResidualIndexHeuristic> input = Arrays.asList(
			new ResidualIndexHeuristic("id-1", 1, 2),
			new ResidualIndexHeuristic("id-2", 10, 10)
		);
		
		List<String> actual = extractCandidates(input, 0.5);
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void testExtractionOfCandidatesWithDifferentLength() {
		List<ResidualIndexHeuristic> input = Arrays.asList(
			new ResidualIndexHeuristic("id-1", 1, 2),
			new ResidualIndexHeuristic("id-2", 10, 10),
			new ResidualIndexHeuristic("id-3", 100, 100),
			new ResidualIndexHeuristic("id-4", 1000, 1000),
			new ResidualIndexHeuristic("id-5", 10000, 10000),
			new ResidualIndexHeuristic("id-6", 100000, 100000)
		);
		
		List<String> actual = extractCandidates(input, 0.5);
		
		Assert.assertEquals(6, actual.size());
		for(String i: actual) {
			Assert.assertTrue(i.contains("\"pairsToCalculate\":[]}"));
		}
	}
	
	@Test
	public void approveExtractionOfCandidatesWithSameLength() {
		List<ResidualIndexHeuristic> input = Arrays.asList(
			new ResidualIndexHeuristic("id-1", 1, 2),
			new ResidualIndexHeuristic("id-2", 10, 10),
			new ResidualIndexHeuristic("id-3", 100, 100),
			new ResidualIndexHeuristic("id-4", 99, 101),
			new ResidualIndexHeuristic("id-5", 98, 102),
			new ResidualIndexHeuristic("id-6", 97, 103)
		);

		List<String> actual = extractCandidates(input, 0.5);
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void testExtractionOfCandidatesWithSameLength() {
		List<ResidualIndexHeuristic> input = Arrays.asList(
			new ResidualIndexHeuristic("id-1", 1, 2),
			new ResidualIndexHeuristic("id-2", 10, 10),
			new ResidualIndexHeuristic("id-3", 100, 100),
			new ResidualIndexHeuristic("id-4", 48, 101),
			new ResidualIndexHeuristic("id-5", 48, 102),
			new ResidualIndexHeuristic("id-6", 48, 103)
		);

		List<String> actual = extractCandidates(input, 0.5);
		
		Assert.assertEquals(6, actual.size());
		for(String i: actual) {
			Assert.assertTrue(i.contains("\"pairsToCalculate\":[]}"));
		}
	}
	
	private List<String> extractCandidates(List<ResidualIndexHeuristic> input, double threshold) {
		List<String> ret = new ArrayList<>(ResidualIndexHeuristics.extractCandidates(jsc(), input, threshold).collect());
		Collections.sort(ret);
		return ret;
	}
}
