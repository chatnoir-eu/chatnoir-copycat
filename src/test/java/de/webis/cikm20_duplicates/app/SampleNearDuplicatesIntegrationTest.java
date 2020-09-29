package de.webis.cikm20_duplicates.app;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple3;

public class SampleNearDuplicatesIntegrationTest {
	@Test
	public void approveSampleForCC17() {
		String firstId = "<urn:uuid:177caa19-fd12-48dd-b85f-aa9151e1048e>";
		String secondId = "<urn:uuid:fd3341db-5dbb-45fe-b5c3-7aa1f91527a6>";
		int hemmingDistance = 0;
		
		Tuple3<String, String, Integer> nearDuplicate = new Tuple3<>(firstId, secondId, hemmingDistance);
		
		String actual = SampleNearDuplicates.samplePairToString(nearDuplicate, args().getString("uuidPrefix"), args().getString("uuidIndex"));
		
		Approvals.verify(actual);
	}
	
	@Test
	public void testParse1() {
		String input = "{\"equivalentDocuments\": [\"<urn:uuid:7cf0905e-ed7c-4581-a3b4-f42fcdbecb9e>\",\"<urn:uuid:9e789e9a-e087-4bd3-a85c-baba5d38f85d>\"],\"hash\":[-17301504, 63854, 7405766, 12357632]}";
		String expected = "(<urn:uuid:7cf0905e-ed7c-4581-a3b4-f42fcdbecb9e>,<urn:uuid:9e789e9a-e087-4bd3-a85c-baba5d38f85d>,0)";
		
		String actual = SampleNearDuplicates.parseExactDuplicates(input).toString();
		
		Assert.assertEquals(expected, actual);
	}
	
	private static Namespace args() {
		return SampleNearDuplicates.validArgumentsOrNull(new String[] {
			"--input", "foo-bar",
			"--output", "foo-bar",
			"--uuidPrefix", "commoncrawl",
			"--uuidIndex", "cc1704"
		});
	}
}
