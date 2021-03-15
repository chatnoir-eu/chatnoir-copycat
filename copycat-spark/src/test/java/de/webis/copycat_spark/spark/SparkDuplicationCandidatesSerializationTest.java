package de.webis.copycat_spark.spark;

import java.util.Arrays;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.copycat_spark.spark.SparkCreateDeduplicationCandidates.DeduplicationUnit;
import de.webis.copycat_spark.util.ClientLocalDeduplication;
import de.webis.copycat_spark.util.ClientLocalDeduplicationTest;
import scala.Tuple2;

public class SparkDuplicationCandidatesSerializationTest extends SparkIntegrationTestBase {

	@Test
	public void testWithEmptyDocuments() {
		Iterable<Tuple2<Integer, DeduplicationUnit>> input = ClientLocalDeduplicationTest.firstLargeDeduplicationInput();
		
		List<String> actual = SparkCreateSourceDocumentsIntegrationTest.sorted(jsc().parallelize(Arrays.asList(input))
			.flatMap(i -> ClientLocalDeduplication.workingPackages(i)));
	
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void testWithSecondExample() {
		Iterable<Tuple2<Integer, DeduplicationUnit>> input = ClientLocalDeduplicationTest.secondLargeDeduplicationInput();
		
		List<String> actual = SparkCreateSourceDocumentsIntegrationTest.sorted(jsc().parallelize(Arrays.asList(input))
			.flatMap(i -> ClientLocalDeduplication.workingPackages(i)));
	
		Approvals.verifyAsJson(actual);
	}
}
