package de.webis.sigir2021.trec;

import java.util.List;
import java.util.Map;

import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_eval.EvaluationMeasure;

public class TransferredSharedTasks {
	public static final SharedTask WEB_2009_DUPLICATE_FREE = task("/artificial-qrels/qrels.inofficial.duplicate-free.web.1-50.txt", 2009),
								  WEB_2010_DUPLICATE_FREE = task("/artificial-qrels/qrels.inofficial.duplicate-free.web.51-100.txt", 2010),
								  WEB_2011_DUPLICATE_FREE = task("/artificial-qrels/qrels.inofficial.duplicate-free.web.101-150.txt", 2011),
								  WEB_2012_DUPLICATE_FREE = task("/artificial-qrels/qrels.inofficial.duplicate-free.web.151-200.txt", 2012);
	
	public static final SharedTask TRANSFER_FROM_WEB_2009_TO_CW_2012 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.1-50.txt", 2009),
								  TRANSFER_FROM_WEB_2010_TO_CW_2012 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.51-100.txt", 2010),
								  TRANSFER_FROM_WEB_2011_TO_CW_2012 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.101-150.txt", 2011),
								  TRANSFER_FROM_WEB_2012_TO_CW_2012 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.151-200.txt", 2012);
	

	public static final SharedTask TRANSFER_FROM_WEB_2009_TO_CW_WB_2012 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12wb12.1-50.txt", 2009),
								  TRANSFER_FROM_WEB_2010_TO_CW_WB_2012 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12wb12.51-100.txt", 2010),
								  TRANSFER_FROM_WEB_2011_TO_CW_WB_2012 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12wb12.101-150.txt", 2011),
								  TRANSFER_FROM_WEB_2012_TO_CW_WB_2012 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12wb12.151-200.txt", 2012);

	public static final SharedTask WEB_2013_DUPLICATE_FREE = task("/artificial-qrels/qrels.inofficial.duplicate-free.web.201-250.txt", 2013),
								  WEB_2014_DUPLICATE_FREE = task("/artificial-qrels/qrels.inofficial.duplicate-free.web.251-300.txt", 2014);
	

	public static final SharedTask TRANSFER_FROM_WEB_2009_TO_CC_2015 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.1-50.txt", 2009),
								  TRANSFER_FROM_WEB_2010_TO_CC_2015 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.51-100.txt", 2010),
								  TRANSFER_FROM_WEB_2011_TO_CC_2015 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.101-150.txt", 2011),
								  TRANSFER_FROM_WEB_2012_TO_CC_2015 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.151-200.txt", 2012),
								  TRANSFER_FROM_WEB_2013_TO_CC_2015 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.201-250.txt", 2013),
								  TRANSFER_FROM_WEB_2014_TO_CC_2015 = task("/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.251-300.txt", 2014);
	
	private static SharedTask task(String qrelResource, int year) {
		return new SharedTask() {
			@Override
			public String getQrelResource() {
				return qrelResource;
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
				return TrecSharedTask.valueOf("WEB_" + year).topicNumberToTopic();
			}
		};
	}
}
