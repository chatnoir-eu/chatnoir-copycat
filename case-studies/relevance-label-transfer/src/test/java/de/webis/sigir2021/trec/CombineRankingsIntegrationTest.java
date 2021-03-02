package de.webis.sigir2021.trec;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.sigir2021.trec.CombineRankings;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_eval.EvaluationMeasure;

public class CombineRankingsIntegrationTest {
	@Test
	public void approveAllFieldsSetToZero() {
		File runDir = new File("src/test/resources/sample-run-dir");
		String prefix = "webis_warc_clueweb12_011";
		SharedTask task = testSharedTask();
		
		String combinedRanking = new CombineRankings(runDir, prefix, task).combine(0, 0, 0);
		
		Approvals.verify(combinedRanking);
	}
	
	@Test
	public void approveMetaWeight1() {
		File runDir = new File("src/test/resources/sample-run-dir");
		String prefix = "webis_warc_clueweb12_011";
		SharedTask task = testSharedTask();
		
		String combinedRanking = new CombineRankings(runDir, prefix, task).combine(0, 0, 1);
		
		Approvals.verify(combinedRanking);
	}
	
	@Test
	public void approveTitleWeight1() {
		File runDir = new File("src/test/resources/sample-run-dir");
		String prefix = "webis_warc_clueweb12_011";
		SharedTask task = testSharedTask();
		
		String combinedRanking = new CombineRankings(runDir, prefix, task).combine(0, 1, 0);
		
		Approvals.verify(combinedRanking);
	}
	
	@Test
	public void approveBodyWeight1() {
		File runDir = new File("src/test/resources/sample-run-dir");
		String prefix = "webis_warc_clueweb12_011";
		SharedTask task = testSharedTask();
		
		String combinedRanking = new CombineRankings(runDir, prefix, task).combine(1, 0, 0);
		
		Approvals.verify(combinedRanking);
	}
	
	@Test
	public void approveAllFields1() {
		File runDir = new File("src/test/resources/sample-run-dir");
		String prefix = "webis_warc_clueweb12_011";
		SharedTask task = testSharedTask();
		
		String combinedRanking = new CombineRankings(runDir, prefix, task).combine(1, 1, 1);
		
		Approvals.verify(combinedRanking);
	}

	private static SharedTask testSharedTask() {
		return new SharedTask() {
			@Override
			public String getQrelResource() {
				return "/sample-run-dir/qrels-test.txt";
			}

			@Override
			public List<EvaluationMeasure> getOfficialEvaluationMeasures() {
				throw new RuntimeException("TBD");
			}

			@Override
			public List<EvaluationMeasure> getInofficialEvaluationMeasures() {
				throw new RuntimeException("TBD");
			}

			@Override
			public List<String> runFiles() {
				throw new RuntimeException("TBD");
			}

			@Override
			public String name() {
				throw new RuntimeException("TBD");
			}

			@Override
			public Map<String, Map<String, String>> topicNumberToTopic() {
				return null;
			}
		};
	}
}
