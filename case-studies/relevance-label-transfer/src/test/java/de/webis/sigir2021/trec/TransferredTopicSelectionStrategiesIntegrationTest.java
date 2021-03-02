package de.webis.sigir2021.trec;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.trec.TransferredSharedTasks;
import de.webis.sigir2021.trec.TransferredTopicSelectionStrategies;
import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;

public class TransferredTopicSelectionStrategiesIntegrationTest {
	private static final AtomicInteger DOC_ID_COUNTER = new AtomicInteger(0);
	
	@Test(expected = IllegalArgumentException.class)
	public void testThatMisplacedArgumentOrderThrowsException() {
		TransferredTopicSelectionStrategies.enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012,
			TransferredSharedTasks.WEB_2009_DUPLICATE_FREE
		);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testThatMisplacedNonOverlappingArgumentOrderThrowsException() {
		TransferredTopicSelectionStrategies.enrichTopicsWithSelectionCriteria(
				TransferredSharedTasks.WEB_2010_DUPLICATE_FREE,
				TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012
		);
	}
	
	@Test
	public void approveAbsoluteJudgmentCountSelectionStrategy() {
		List<String> selectedTopics = TransferredTopicSelectionStrategies.enrichTopicsWithSelectionCriteria(
			TransferredSharedTasks.WEB_2009_DUPLICATE_FREE,
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012
		);
		
		Approvals.verifyAsJson(selectedTopics);
	}
	
	@Test
	public void checkThatCorrectPermutationsAreCalculatedForQrelsWithOnlyPositiveLabels() {
		Set<QrelEqualWithoutScore> qrels = qrels(q("1", 0), q("1", 0),
				q("1", 0), q("1", 0), q("1", 0), q("1", 0),
				q("1", 0), q("1", 0), q("1", 0), q("1", 0),
				q("1", 0), q("1", 0), q("1", 0), q("1", 0),
				q("1", 0), q("1", 0), q("1", 0), q("1", 0));
		
		BigInteger expected = BigInteger.ONE;
		BigInteger actual = TransferredTopicSelectionStrategies.kPermutations(qrels, 1);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkThatCorrectPermutationsAreCalculatedForEmptyQrels() {
		Set<QrelEqualWithoutScore> qrels = qrels(q("1", 0), q("1", 0),
				q("1", 0), q("1", 0), q("1", 0), q("1", 0),
				q("1", 0), q("1", 0), q("1", 0), q("1", 0),
				q("1", 0), q("1", 0), q("1", 0), q("1", 0),
				q("1", 0), q("1", 0), q("1", 0), q("1", 0));
		
		BigInteger expected = BigInteger.ONE;
		BigInteger actual = TransferredTopicSelectionStrategies.kPermutations(qrels, 2);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkThatCorrectPermutationsAreCalculatedForMississippiExample() {
		// textbook: permutations of mississippi = 34650
		int m = 1, i = 2, s = 3, p = 4;
		Set<QrelEqualWithoutScore> qrels = qrels(q("1", m), q("1", i),
				q("1", s), q("1", s), q("1", i), q("1", s),
				q("1", s), q("1", i), q("1", p), q("1", p),
				q("1", i));
		
		BigInteger expected = BigInteger.valueOf(34650);
		BigInteger actual = TransferredTopicSelectionStrategies.kPermutations(qrels, 1);

		Assert.assertEquals(expected, actual);
	}
	
	private static Set<QrelEqualWithoutScore> qrels(QrelEqualWithoutScore...qrels) {
		return Arrays.asList(qrels).stream().collect(Collectors.toSet());
	}
	
	private static synchronized QrelEqualWithoutScore q(String topic, int label) {
		QrelEqualWithoutScore ret = new QrelEqualWithoutScore();
		ret.setTopicNumber(Integer.valueOf(topic));
		ret.setScore(label);
		ret.setDocumentID(DOC_ID_COUNTER.decrementAndGet() + "");
		
		return ret;
	}
}
