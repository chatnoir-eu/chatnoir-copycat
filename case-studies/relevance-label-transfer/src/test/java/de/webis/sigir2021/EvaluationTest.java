package de.webis.sigir2021;

import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.Evaluation;

public class EvaluationTest {
	@Test
	public void extractionOfCw12Id() {
		Assert.assertEquals("clueweb12-1716wb-71-08211", Evaluation.cw12Id("{\"firstId\":\"<urn:uuid:d0c4d7ae-b316-416e-8b45-9a5d8942a0c1>\",\"secondId\":\"clueweb12-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
		Assert.assertNull(Evaluation.cw12Id("{\"firstId\":\"<urn:uuid:d0c4d7ae-b316-416e-8b45-9a5d8942a0c1>\",\"secondId\":\"ss-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
		Assert.assertEquals("clueweb12-s", Evaluation.cw12Id("{\"firstId\":\"clueweb12-s\",\"secondId\":\"clueweb12-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
	}
	
	@Test
	public void extractionOfCw09Id() {
		Assert.assertEquals("clueweb09-1716wb-71-08211", Evaluation.cw09Id("{\"firstId\":\"<urn:uuid:d0c4d7ae-b316-416e-8b45-9a5d8942a0c1>\",\"secondId\":\"clueweb09-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
		Assert.assertNull(Evaluation.cw09Id("{\"firstId\":\"<urn:uuid:d0c4d7ae-b316-416e-8b45-9a5d8942a0c1>\",\"secondId\":\"ss-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
		Assert.assertEquals("clueweb09-s", Evaluation.cw09Id("{\"firstId\":\"clueweb09-s\",\"secondId\":\"clueweb12-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
	}
	
	
	@Test
	public void extractionOfCC15Id() {
		Assert.assertEquals("<urn:uuid:d0c4d7ae-b316-416e-8b45-9a5d8942a0c1>", Evaluation.cc15Id("{\"firstId\":\"<urn:uuid:d0c4d7ae-b316-416e-8b45-9a5d8942a0c1>\",\"secondId\":\"clueweb09-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
		Assert.assertEquals("<urn:uuid:d0c4d7ae-b316-416e-8b45-9a5d8942a0c1>", Evaluation.cc15Id("{\"firstId\":\"clueweb12ss-1716wb-71-08211\",\"secondId\":\"<urn:uuid:d0c4d7ae-b316-416e-8b45-9a5d8942a0c1>\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
		Assert.assertNull(Evaluation.cc15Id("{\"firstId\":\"clueweb09-s\",\"secondId\":\"clueweb12-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
	}
	
	@Test
	public void extractionOfCw09OrCw12Id() {
		Assert.assertNull(Evaluation.cw09OrCw12("{\"firstId\":\"<urn:uuid:d0c4d7ae-b316-416e-8b45-9a5d8942a0c1>\",\"secondId\":\"clsueweb09-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
		Assert.assertEquals("clueweb12ss-1716wb-71-08211", Evaluation.cw09OrCw12("{\"firstId\":\"<urn:uuid:d0c4d7ae-b316-416e-8b45-9a5d8942a0c1>\",\"secondId\":\"clueweb12ss-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
		Assert.assertEquals("clueweb09-s", Evaluation.cw09OrCw12("{\"firstId\":\"clueweb09-s\",\"secondId\":\"clueweb12-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"));
	}
	
	@Test
	public void extractionOfS3Score() {
		Assert.assertEquals(0.000000d, Evaluation.s3Score("{\"firstId\":\"<urn:uuid:d0c4d7ae-b316-416e-8b45-9a5d8942a0c1>\",\"secondId\":\"clueweb09-1716wb-71-08211\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"), 0.0001d);
		Assert.assertEquals(0.321d, Evaluation.s3Score("{\"firstId\":\"clueweb09-s\",\"secondId\":\"clueweb12-1716wb-71-08211\",\"s3Score\":0.321,\"cosineSimilarityOneGramms\":0.7434,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0054}"), 0.0001);
	}
}
