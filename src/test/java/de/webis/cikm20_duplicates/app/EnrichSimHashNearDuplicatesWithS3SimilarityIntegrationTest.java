package de.webis.cikm20_duplicates.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.app.EnrichSimHashNearDuplicatesWithS3Similarity.DocumentResolverFactory;
import de.webis.cikm20_duplicates.spark.SparkIntegrationTestBase;
import net.sourceforge.argparse4j.inf.Namespace;

public class EnrichSimHashNearDuplicatesWithS3SimilarityIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void approveEmptyNearDuplicateInput() {
		JavaRDD<String> simHashNearDuplicates = simHashNearDuplicates();
		List<String> expected = Arrays.asList();
		JavaRDD<String> actual = EnrichSimHashNearDuplicatesWithS3Similarity.enrichNearDuplicatesWithS3Score(simHashNearDuplicates, cw09Resolver());

		assertEquals(expected, actual);
	}
	
	@Test
	public void approveSmallSampleInputWithOnlyIllegalDocumentIds() {
		JavaRDD<String> simHashNearDuplicates = simHashNearDuplicates(
			"clueweb09-en0058-20-30937,clueweb09-en0060-58-25055",
			"clueweb09-en0019-66-11581,clueweb09-en0019-66-12725",
			"clueweb09-en0058-20-30937,clueweb09-en0019-66-12725"
		);
		List<String> expected = Arrays.asList(
			"clueweb09-en0058-20-30937,clueweb09-en0060-58-25055,0.8750",
			"clueweb09-en0019-66-11581,clueweb09-en0019-66-12725,0.9253",
			"clueweb09-en0058-20-30937,clueweb09-en0019-66-12725,0.0000"
		);
		JavaRDD<String> actual = EnrichSimHashNearDuplicatesWithS3Similarity.enrichNearDuplicatesWithS3Score(simHashNearDuplicates, cw09Resolver());
		
		assertEquals(expected, actual);
	}
	
	@Test
	public void approveSmallSampleInputWithMultipleIds() {
		JavaRDD<String> simHashNearDuplicates = simHashNearDuplicates(
			"non-existing-id-1,non-existing-id-2",
			"non-existing-id-1,non-existing-id-3"
		);
		List<String> expected = Arrays.asList(
			"non-existing-id-1,non-existing-id-2,-1",
			"non-existing-id-1,non-existing-id-3,-1"
		);
		JavaRDD<String> actual = EnrichSimHashNearDuplicatesWithS3Similarity.enrichNearDuplicatesWithS3Score(simHashNearDuplicates, cw09Resolver());
		
		assertEquals(expected, actual);
	}
	
	private static void assertEquals(List<String> expected, JavaRDD<String> actual) {
		expected = new ArrayList<>(expected);
		List<String> actualList = new ArrayList<>(actual.collect());
		
		Collections.sort(expected);
		Collections.sort(actualList);
		
		Assert.assertEquals(expected, actualList);
	}
	
	private static DocumentResolverFactory cw09Resolver() {
		Namespace args = EnrichSimHashNearDuplicatesWithS3Similarity.validArgumentsOrNull(new String[] {
			"-i", "foo",
			"-o", "bar",
			"--uuidPrefix", "clueweb09",
			"--uuidIndex", "cw09",
		});
		
		return EnrichSimHashNearDuplicatesWithS3Similarity.docResolver(args);
	}

	private JavaRDD<String> simHashNearDuplicates(String...elements) {
		return jsc().parallelize(Arrays.asList(elements));
	}
}
