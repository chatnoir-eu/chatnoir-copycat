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
	
	@Test
	public void example2() {
		String expected = "www.sfigf.com";
		String actual = SparkAnalyzeCanonicalLinkGraph.hostFromUrl("http://www.sfigf.com/504/portland_christmas_cash_and_carry_show_|_booth_package.htm");
		
		Assert.assertEquals(expected, actual);
	}
}
