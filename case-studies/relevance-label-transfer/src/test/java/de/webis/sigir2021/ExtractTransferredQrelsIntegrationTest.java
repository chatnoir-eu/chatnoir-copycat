package de.webis.sigir2021;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.ExtractTransferredQrels;
import de.webis.sigir2021.trec.TransferredSharedTasks;

public class ExtractTransferredQrelsIntegrationTest {

	@Test
	public void approveTransformationForWebTrack2012() {
		String actual = ExtractTransferredQrels.extractTransferredQrels(TransferredSharedTasks.WEB_2009_DUPLICATE_FREE, "cw12");
		Approvals.verify(actual);
	}
	
	@Test
	public void approveTargetPathForWebTrack2012Sample() {
		String actual = ExtractTransferredQrels.targetPath(TransferredSharedTasks.WEB_2009_DUPLICATE_FREE, "cw12").toString();
		
		Assert.assertEquals("src/main/resources/artificial-qrels/qrels-with-only-transferred-original-labels/qrels.inofficial.duplicate-free.transferred-to-cw12.1-50.txt", actual);
	}
	
	@Test
	public void approveTransformationForCC15() {
		String actual = ExtractTransferredQrels.extractTransferredQrels(TransferredSharedTasks.WEB_2009_DUPLICATE_FREE, "cc15");
		Approvals.verify(actual);
	}
}
