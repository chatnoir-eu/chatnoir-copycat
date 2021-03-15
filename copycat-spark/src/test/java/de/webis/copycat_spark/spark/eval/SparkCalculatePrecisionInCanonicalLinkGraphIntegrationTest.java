package de.webis.copycat_spark.spark.eval;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.spark.eval.SparkCalculatePrecisionInCanonicalLinkGraph;

public class SparkCalculatePrecisionInCanonicalLinkGraphIntegrationTest {
	@Test
	public void testAllFeaturesAreUsed() {
		List<String> expected = Arrays.asList("1-gramms", "3-gramms", "5-gramms", "8-gramms",
				"1-3-gramms", "1-5-gramms", "1-8-gramms", "3-5-gramms", "3-8-gramms", "5-8-gramms"
		);
		List<String> actual = SparkCalculatePrecisionInCanonicalLinkGraph.featureNames();
		
		Assert.assertEquals(expected, actual);
	}
}
