package de.webis.sigir2021.trec;

import java.util.List;
import java.util.Map;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.sigir2021.trec.LabelTransfer;
import de.webis.sigir2021.trec.LabelTransfer.TargetDocument;

public class LabelTransferTest {
	@Test
	public void approveSmallTransferSampleOWaybackMachine() {
		String input = "src/test/resources/sample-wayback-similarities.jsonl";
		Map<String, List<TargetDocument>> actual = LabelTransfer.waybackNearDuplicates(input);
		
		Approvals.verifyAsJson(actual);
	}
}
