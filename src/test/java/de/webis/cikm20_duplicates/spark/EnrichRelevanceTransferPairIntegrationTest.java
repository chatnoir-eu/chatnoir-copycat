package de.webis.cikm20_duplicates.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Test;

public class EnrichRelevanceTransferPairIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void approveEnrichmentOfExampleRelevanceTransferPair() {
		JavaRDD<String> input = rdd(
			"{\"srcId\":\"clueweb09-en0000-79-31634\",\"targetId\":\"clueweb12-0900wb-15-08700\",\"topic\":\"CLUEWEB09::ENTITY_2010::25\",\"srcURL\":\"https://chatnoir.eu/cache?uuid=1dbc1f0b-f23c-5107-a5f8-464bd5e26adf&index=cw09&raw\",\"targetURL\":\"https://chatnoir.eu/cache?uuid=f3539b18-9497-57c2-a8d2-91f6e5e161f4&index=cw12&raw\",\"k\":0,\"relevanceLabel\":1}",
			"{\"srcId\":\"clueweb09-en0000-79-31634\",\"targetId\":\"clueweb09-en0000-79-31634\",\"topic\":\"CLUEWEB09::ENTITY_2010::25\",\"k\":0,\"relevanceLabel\":1}"
		);
		List<String> actual = enrich(input);
		Approvals.verifyAsJson(actual);
	}

	private JavaRDD<String> rdd(String...srcs) {
		return jsc().parallelize(Arrays.asList(srcs));
	}
	
	private List<String> enrich(JavaRDD<String> input) {
		JavaRDD<String> ret = SparkEnrichRelevanceTransferPairs.enrich(input);
		return SparkCreateSourceDocumentsIntegrationTest.sorted(ret);
	}
}
