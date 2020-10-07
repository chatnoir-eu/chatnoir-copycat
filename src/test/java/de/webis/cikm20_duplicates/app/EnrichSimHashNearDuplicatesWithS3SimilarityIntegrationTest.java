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
			"{\"firstId\":\"clueweb09-en0058-20-30937\",\"secondId\":\"clueweb09-en0060-58-25055\",\"hemmingDistance\":-1}",
			"{\"firstId\":\"clueweb09-en0019-66-11581\",\"secondId\":\"clueweb09-en0019-66-12725\",\"hemmingDistance\":-1}",
			"{\"firstId\":\"clueweb09-en0058-20-30937\",\"secondId\":\"clueweb09-en0019-66-12725\",\"hemmingDistance\":-1}"
		);
		List<String> expected = Arrays.asList(
			"{\"firstId\":\"clueweb09-en0058-20-30937\",\"secondId\":\"clueweb09-en0060-58-25055\",\"s3Score\":0.8750,\"cosineSimilarityOneGramms\":0.9869,\"cosineSimilarityEightGramms\":0.8751,\"cosineSimilarityThreeAndFiveGramms\":0.9203}",
			"{\"firstId\":\"clueweb09-en0019-66-11581\",\"secondId\":\"clueweb09-en0019-66-12725\",\"s3Score\":0.9253,\"cosineSimilarityOneGramms\":0.9825,\"cosineSimilarityEightGramms\":0.9253,\"cosineSimilarityThreeAndFiveGramms\":0.9472}",
			"{\"firstId\":\"clueweb09-en0058-20-30937\",\"secondId\":\"clueweb09-en0019-66-12725\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.0607,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0000}"
		);
		JavaRDD<String> actual = EnrichSimHashNearDuplicatesWithS3Similarity.enrichNearDuplicatesWithS3Score(simHashNearDuplicates, cw09Resolver());
		
		assertEquals(expected, actual);
	}
	
	@Test
	public void approveSmallSampleWithIdsFromDifferentCorpora() {
		JavaRDD<String> simHashNearDuplicates = simHashNearDuplicates(
				"{\"firstId\":\"<urn:uuid:0eab5a9c-5573-434f-9ac1-df211d86b1bb>\",\"secondId\":\"<urn:uuid:11b6169b-38b5-4f3a-814c-63d84b3d8ade>\",\"hemmingDistance\":3}",
				"{\"firstId\":\"clueweb09-pt0004-98-29216\",\"secondId\":\"clueweb09-pt0006-67-33686\",\"hemmingDistance\":3}",
				"{\"firstId\":\"<urn:uuid:8a19d9ee-02f4-4239-988a-8e44586020c0>\",\"secondId\":\"clueweb09-en0114-89-22558\",\"hemmingDistance\":3}",
				"{\"firstId\":\"clueweb09-en0081-04-12999\",\"secondId\":\"clueweb12-0401wb-82-19222\",\"hemmingDistance\":2}"
			);
		List<String> expected = Arrays.asList(
			"{\"firstId\":\"<urn:uuid:0eab5a9c-5573-434f-9ac1-df211d86b1bb>\",\"secondId\":\"<urn:uuid:11b6169b-38b5-4f3a-814c-63d84b3d8ade>\",\"s3Score\":0.7270,\"cosineSimilarityOneGramms\":0.9868,\"cosineSimilarityEightGramms\":0.7730,\"cosineSimilarityThreeAndFiveGramms\":0.9192}",
			"{\"firstId\":\"<urn:uuid:8a19d9ee-02f4-4239-988a-8e44586020c0>\",\"secondId\":\"clueweb09-en0114-89-22558\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.6553,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0012}",
			"{\"firstId\":\"clueweb09-en0081-04-12999\",\"secondId\":\"clueweb12-0401wb-82-19222\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.7187,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0157}",
			"{\"firstId\":\"clueweb09-pt0004-98-29216\",\"secondId\":\"clueweb09-pt0006-67-33686\",\"s3Score\":0.0000,\"cosineSimilarityOneGramms\":0.6690,\"cosineSimilarityEightGramms\":0.0000,\"cosineSimilarityThreeAndFiveGramms\":0.0000}"
		);
		
		JavaRDD<String> actual = EnrichSimHashNearDuplicatesWithS3Similarity.enrichNearDuplicatesWithS3Score(simHashNearDuplicates, cw09Resolver());
		
		assertEquals(expected, actual);
	}
	
	@Test
	public void approveSmallSampleInputWithMultipleIds() {
		JavaRDD<String> simHashNearDuplicates = simHashNearDuplicates(
			"{\"firstId\":\"non-existing-id-1\",\"secondId\":\"non-existing-id-2\",\"hemmingDistance\":-1}",
			"{\"firstId\":\"non-existing-id-1\",\"secondId\":\"non-existing-id-3\",\"hemmingDistance\":-1}"
		);
		List<String> expected = Arrays.asList(
			"{\"firstId\":\"non-existing-id-1\",\"secondId\":\"non-existing-id-2\",\"s3Score\":-1,\"cosineSimilarityOneGramms\":-1,\"cosineSimilarityEightGramms\":-1,\"cosineSimilarityThreeAndFiveGramms\":-1}",
			"{\"firstId\":\"non-existing-id-1\",\"secondId\":\"non-existing-id-3\",\"s3Score\":-1,\"cosineSimilarityOneGramms\":-1,\"cosineSimilarityEightGramms\":-1,\"cosineSimilarityThreeAndFiveGramms\":-1}"
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
