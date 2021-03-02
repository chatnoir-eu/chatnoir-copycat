package de.webis.sigir2021;

import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.CreateOutputForRankingExperiments.CNDocument;
import de.webis.sigir2021.trec.TransferredSharedTasks;

public class CreateOutputForRankingExperimentsTest {
	@Test(expected = IllegalArgumentException.class)
	public void testThatNullIsInvalidSharedTask() {
		new CNDocument("clueweb12-0602wb-00-26183", null);
	}
	
	@Test
	public void testCorrectCW12Document() {
		Assert.assertEquals("clueweb12-0602wb-00-26183", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2009_DUPLICATE_FREE).getTrecId());
		Assert.assertEquals("fff47d5e-5630-513f-a3e3-7d7d0dec8d20", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2009_DUPLICATE_FREE).getChatNoirId());
		Assert.assertEquals("webis_warc_clueweb12_011", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2009_DUPLICATE_FREE).getIndex());
		
		
		Assert.assertEquals("clueweb12-0602wb-00-26183", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2010_DUPLICATE_FREE).getTrecId());
		Assert.assertEquals("fff47d5e-5630-513f-a3e3-7d7d0dec8d20", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2010_DUPLICATE_FREE).getChatNoirId());
		Assert.assertEquals("webis_warc_clueweb12_011", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2010_DUPLICATE_FREE).getIndex());
		
		Assert.assertEquals("clueweb12-0602wb-00-26183", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2011_DUPLICATE_FREE).getTrecId());
		Assert.assertEquals("fff47d5e-5630-513f-a3e3-7d7d0dec8d20", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2011_DUPLICATE_FREE).getChatNoirId());
		Assert.assertEquals("webis_warc_clueweb12_011", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2011_DUPLICATE_FREE).getIndex());
		
		Assert.assertEquals("clueweb12-0602wb-00-26183", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2012_DUPLICATE_FREE).getTrecId());
		Assert.assertEquals("fff47d5e-5630-513f-a3e3-7d7d0dec8d20", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2012_DUPLICATE_FREE).getChatNoirId());
		Assert.assertEquals("webis_warc_clueweb12_011", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2012_DUPLICATE_FREE).getIndex());
		
		Assert.assertEquals("clueweb12-0602wb-00-26183", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012).getTrecId());
		Assert.assertEquals("fff47d5e-5630-513f-a3e3-7d7d0dec8d20", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012).getChatNoirId());
		Assert.assertEquals("webis_warc_clueweb12_011", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012).getIndex());
		
		Assert.assertEquals("clueweb12-0602wb-00-26183", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_2012).getTrecId());
		Assert.assertEquals("fff47d5e-5630-513f-a3e3-7d7d0dec8d20", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_2012).getChatNoirId());
		Assert.assertEquals("webis_warc_clueweb12_011", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_2012).getIndex());
		
		Assert.assertEquals("clueweb12-0602wb-00-26183", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_2012).getTrecId());
		Assert.assertEquals("fff47d5e-5630-513f-a3e3-7d7d0dec8d20", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_2012).getChatNoirId());
		Assert.assertEquals("webis_warc_clueweb12_011", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_2012).getIndex());
		
		Assert.assertEquals("clueweb12-0602wb-00-26183", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_2012).getTrecId());
		Assert.assertEquals("fff47d5e-5630-513f-a3e3-7d7d0dec8d20", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_2012).getChatNoirId());
		Assert.assertEquals("webis_warc_clueweb12_011", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_2012).getIndex());
		
		Assert.assertEquals("clueweb12-0602wb-00-26183", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2013_DUPLICATE_FREE).getTrecId());
		Assert.assertEquals("fff47d5e-5630-513f-a3e3-7d7d0dec8d20", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2013_DUPLICATE_FREE).getChatNoirId());
		Assert.assertEquals("webis_warc_clueweb12_011", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2013_DUPLICATE_FREE).getIndex());

		Assert.assertEquals("clueweb12-0602wb-00-26183", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2014_DUPLICATE_FREE).getTrecId());
		Assert.assertEquals("fff47d5e-5630-513f-a3e3-7d7d0dec8d20", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2014_DUPLICATE_FREE).getChatNoirId());
		Assert.assertEquals("webis_warc_clueweb12_011", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.WEB_2014_DUPLICATE_FREE).getIndex());
	}
	
	@Test(expected = RuntimeException.class)
	public void testIncorrectCW12Document() {
		new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.WEB_2009_DUPLICATE_FREE);
	}
	
	@Test
	public void testCorrectWB12Document() {
		Assert.assertEquals("clueweb12-0602wb-00-26183", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getTrecId());
		Assert.assertEquals("fff47d5e-5630-513f-a3e3-7d7d0dec8d20", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getChatNoirId());
		Assert.assertEquals("clueweb12-and-wayback12", new CNDocument("clueweb12-0602wb-00-26183", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getIndex());
	}
	
	@Test
	public void testCorrectWB12Document2() {
		Assert.assertEquals("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getTrecId());
		Assert.assertEquals("9158cfd3-e0e6-5424-ba81-365ecf12f804", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getChatNoirId());
		Assert.assertEquals("clueweb12-and-wayback12", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getIndex());
		
		Assert.assertEquals("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getTrecId());
		Assert.assertEquals("9158cfd3-e0e6-5424-ba81-365ecf12f804", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getChatNoirId());
		Assert.assertEquals("clueweb12-and-wayback12", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_WB_2012).getIndex());
		
		Assert.assertEquals("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getTrecId());
		Assert.assertEquals("9158cfd3-e0e6-5424-ba81-365ecf12f804", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getChatNoirId());
		Assert.assertEquals("clueweb12-and-wayback12", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_WB_2012).getIndex());
		
		Assert.assertEquals("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getTrecId());
		Assert.assertEquals("9158cfd3-e0e6-5424-ba81-365ecf12f804", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getChatNoirId());
		Assert.assertEquals("clueweb12-and-wayback12", new CNDocument("<urn:uuid:5780b0cb-219c-48f3-ad41-fdb2b3d7ad15>", TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_WB_2012).getIndex());
	}
	
	@Test
	public void testCorrectWB12Document3() {
		Assert.assertEquals("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getTrecId());
		Assert.assertEquals("7cca662b-0028-5f96-8445-b2e7d2d74cd4", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getChatNoirId());
		Assert.assertEquals("clueweb12-and-wayback12", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getIndex());
		
		Assert.assertEquals("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getTrecId());
		Assert.assertEquals("7cca662b-0028-5f96-8445-b2e7d2d74cd4", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getChatNoirId());
		Assert.assertEquals("clueweb12-and-wayback12", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_WB_2012).getIndex());
		
		Assert.assertEquals("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getTrecId());
		Assert.assertEquals("7cca662b-0028-5f96-8445-b2e7d2d74cd4", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getChatNoirId());
		Assert.assertEquals("clueweb12-and-wayback12", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_WB_2012).getIndex());
		
		Assert.assertEquals("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getTrecId());
		Assert.assertEquals("7cca662b-0028-5f96-8445-b2e7d2d74cd4", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012).getChatNoirId());
		Assert.assertEquals("clueweb12-and-wayback12", new CNDocument("<urn:uuid:19be7832-f2d9-43e7-9e8f-2416e9499f42>", TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_WB_2012).getIndex());
	}
	
	@Test
	public void testCorrectCC15Document() {
		Assert.assertEquals("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CC_2015).getTrecId());
		Assert.assertEquals("33bd42d2-b3e1-597c-ba22-8346c86bad33", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CC_2015).getIndex());
		
		Assert.assertEquals("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CC_2015).getTrecId());
		Assert.assertEquals("33bd42d2-b3e1-597c-ba22-8346c86bad33", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CC_2015).getIndex());
		
		Assert.assertEquals("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CC_2015).getTrecId());
		Assert.assertEquals("33bd42d2-b3e1-597c-ba22-8346c86bad33", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CC_2015).getIndex());
		
		Assert.assertEquals("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CC_2015).getTrecId());
		Assert.assertEquals("33bd42d2-b3e1-597c-ba22-8346c86bad33", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CC_2015).getIndex());
		
		Assert.assertEquals("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2013_TO_CC_2015).getTrecId());
		Assert.assertEquals("33bd42d2-b3e1-597c-ba22-8346c86bad33", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2013_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2013_TO_CC_2015).getIndex());
		
		Assert.assertEquals("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2014_TO_CC_2015).getTrecId());
		Assert.assertEquals("33bd42d2-b3e1-597c-ba22-8346c86bad33", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2014_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:dccd6d43-0f60-45a6-8580-a361ca89bd9b>", TransferredSharedTasks.TRANSFER_FROM_WEB_2014_TO_CC_2015).getIndex());
	}
	
	@Test
	public void testCorrectCC15Document2() {
		Assert.assertEquals("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CC_2015).getTrecId());
		Assert.assertEquals("9dccc751-4e3f-5639-a654-947c7aa4c433", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CC_2015).getIndex());
		
		Assert.assertEquals("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CC_2015).getTrecId());
		Assert.assertEquals("9dccc751-4e3f-5639-a654-947c7aa4c433", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CC_2015).getIndex());
		
		Assert.assertEquals("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CC_2015).getTrecId());
		Assert.assertEquals("9dccc751-4e3f-5639-a654-947c7aa4c433", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CC_2015).getIndex());
		
		Assert.assertEquals("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CC_2015).getTrecId());
		Assert.assertEquals("9dccc751-4e3f-5639-a654-947c7aa4c433", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CC_2015).getIndex());
		
		Assert.assertEquals("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2013_TO_CC_2015).getTrecId());
		Assert.assertEquals("9dccc751-4e3f-5639-a654-947c7aa4c433", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2013_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2013_TO_CC_2015).getIndex());
		
		Assert.assertEquals("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2014_TO_CC_2015).getTrecId());
		Assert.assertEquals("9dccc751-4e3f-5639-a654-947c7aa4c433", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2014_TO_CC_2015).getChatNoirId());
		Assert.assertEquals("webis_warc_commoncrawl15_002", new CNDocument("<urn:uuid:47b6684a-22ce-42e0-be8f-acca073705cd>", TransferredSharedTasks.TRANSFER_FROM_WEB_2014_TO_CC_2015).getIndex());
	}
}
