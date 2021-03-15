package de.webis.copycat_spark.spark.util;

import java.util.Collections;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.copycat_spark.spark.util.SparkRepartitionParts;

public class SparkRepartitionPartsIntegrationTest {
	@Test
	public void testGenerationOfSiffixes() {
		List<String> suffixes = SparkRepartitionParts.createPartSuffixes();
		Collections.sort(suffixes);
		
		Approvals.verifyAsJson(suffixes);
	}
}
