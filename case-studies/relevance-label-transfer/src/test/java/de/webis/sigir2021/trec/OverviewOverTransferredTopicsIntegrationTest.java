package de.webis.sigir2021.trec;

import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.sigir2021.trec.OverviewOverTransferredTopics;
import de.webis.sigir2021.trec.TransferredSharedTasks;

public class OverviewOverTransferredTopicsIntegrationTest {
	@Test(expected = IllegalArgumentException.class)
	public void testThatMisplacedArgumentOrderThrowsException() {
		OverviewOverTransferredTopics.overviewOverTransferredTopics(
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012,
			TransferredSharedTasks.WEB_2009_DUPLICATE_FREE
		);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testThatMisplacedNonOverlappingArgumentOrderThrowsException() {
		OverviewOverTransferredTopics.overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2010_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012
		);
	}
	
	@Test
	public void approveQrelAggregation() {
		List<String> selectedTopics = OverviewOverTransferredTopics.overviewOverTransferredTopics(
			TransferredSharedTasks.WEB_2009_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012
		);
		
		Approvals.verifyAsJson(selectedTopics);
	}
}
