package de.webis.cikm20_duplicates.spark.eval;

import org.junit.Assert;
import org.junit.Test;

public class HostExtractionTest {
	
	@Test
	public void example1() {
		String expected = "forums.penny-arcade.com";
		String actual = SparkAnalyzeCanonicalLinkGraph.hostFromUrl("http://forums.penny-arcade.com/profile/discussions/Bliss 101");
		
		Assert.assertEquals(expected, actual);
	}
}
