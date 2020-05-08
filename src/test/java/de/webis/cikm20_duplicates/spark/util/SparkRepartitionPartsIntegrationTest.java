package de.webis.cikm20_duplicates.spark.util;

import java.util.Collections;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Test;

public class SparkRepartitionPartsIntegrationTest {
	@Test
	public void testGenerationOfSiffixes() {
		List<String> suffixes = SparkRepartitionParts.createPartSuffixes();
		Collections.sort(suffixes);
		
		Approvals.verifyAsJson(suffixes);
	}
}
