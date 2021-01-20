package de.webis.cikm20_duplicates.app;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.app.DeduplicateTrecRunFile.DefaultSimilarityCalculation;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil.DocumentResolver;

public class DeduplicateTrecRunFileIntegrationTest {
	@Test
	public void testOnEmptyRunFile() {
		String runFileContent = "";
		List<String> expected = Arrays.asList();

		List<String> actual = deduplicate(runFileContent);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testOnRunFileWithSingleDuplicatesWithinSingleTopic() {
		String runFileContent = "1 Q0 clueweb09-en0060-58-25055 2 0.0 tag\n"
				+ "1 Q0 clueweb09-en0058-20-30937 2 0.0 tag\n";
		List<String> expected = Arrays.asList(
				"{\"topic\":\"1\",\"similarities\":[{\"firstId\":\"clueweb09-en0058-20-30937\",\"secondId\":\"clueweb09-en0060-58-25055\",\"similarities\":{\"s3Score\":0.875,\"cosineSimilarityOneGramms\":0.9868972,\"cosineSimilarityEightGramms\":0.8750517,\"cosineSimilarityThreeAndFiveGramms\":0.92026484,\"canonicalUrl\":0.0,\"64BitK3SimHashOneGramms\":0.953125,\"64BitK3SimHashThreeAndFiveGramms\":0.96875}}]}");

		List<String> actual = deduplicate(runFileContent);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testOnRunFileWithMultipleDuplicatesWithinSingleTopic() {
		String runFileContent = "1 Q0 clueweb09-en0060-58-25055 2 0.0 tag\n"
				+ "1 Q0 clueweb09-en0058-20-30937 2 0.0 tag\n" + "1 Q0 clueweb09-en0019-66-11581 2 0.0 tag\n";
		List<String> expected = Arrays.asList(
				"{\"topic\":\"1\",\"similarities\":[{\"firstId\":\"clueweb09-en0019-66-11581\",\"secondId\":\"clueweb09-en0058-20-30937\",\"similarities\":{\"s3Score\":0.0,\"cosineSimilarityOneGramms\":0.061168198,\"cosineSimilarityEightGramms\":0.0,\"cosineSimilarityThreeAndFiveGramms\":0.0,\"canonicalUrl\":0.0,\"64BitK3SimHashOneGramms\":0.484375,\"64BitK3SimHashThreeAndFiveGramms\":0.515625}},{\"firstId\":\"clueweb09-en0019-66-11581\",\"secondId\":\"clueweb09-en0060-58-25055\",\"similarities\":{\"s3Score\":0.0,\"cosineSimilarityOneGramms\":0.060868107,\"cosineSimilarityEightGramms\":0.0,\"cosineSimilarityThreeAndFiveGramms\":0.0,\"canonicalUrl\":0.0,\"64BitK3SimHashOneGramms\":0.4375,\"64BitK3SimHashThreeAndFiveGramms\":0.515625}},{\"firstId\":\"clueweb09-en0058-20-30937\",\"secondId\":\"clueweb09-en0060-58-25055\",\"similarities\":{\"s3Score\":0.875,\"cosineSimilarityOneGramms\":0.9868972,\"cosineSimilarityEightGramms\":0.8750517,\"cosineSimilarityThreeAndFiveGramms\":0.92026484,\"canonicalUrl\":0.0,\"64BitK3SimHashOneGramms\":0.953125,\"64BitK3SimHashThreeAndFiveGramms\":0.96875}}]}");

		List<String> actual = deduplicate(runFileContent);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testOnRunFileWithMultipleTopics() {
		String runFileContent = "1 Q0 clueweb09-en0060-58-25055 2 0.0 tag\n"
				+ "2 Q0 clueweb09-en0019-66-11581 2 0.0 tag\n" + "1 Q0 clueweb09-en0058-20-30937 2 0.0 tag\n";
		List<String> expected = Arrays.asList(
				"{\"topic\":\"1\",\"similarities\":[{\"firstId\":\"clueweb09-en0058-20-30937\",\"secondId\":\"clueweb09-en0060-58-25055\",\"similarities\":{\"s3Score\":0.875,\"cosineSimilarityOneGramms\":0.9868972,\"cosineSimilarityEightGramms\":0.8750517,\"cosineSimilarityThreeAndFiveGramms\":0.92026484,\"canonicalUrl\":0.0,\"64BitK3SimHashOneGramms\":0.953125,\"64BitK3SimHashThreeAndFiveGramms\":0.96875}}]}",
				"{\"topic\":\"2\",\"similarities\":[]}");

		List<String> actual = deduplicate(runFileContent);

		Assert.assertEquals(expected, actual);
	}

	private List<String> deduplicate(String runFileContent) {
		DocumentResolver cw09DocResolver = EnrichSimHashNearDuplicatesWithS3SimilarityIntegrationTest.cw09Resolver().get();
		DefaultSimilarityCalculation sim = new DefaultSimilarityCalculation(Arrays.asList("s3Score", "cosineSimilarityOneGramms", "cosineSimilarityEightGramms", "cosineSimilarityThreeAndFiveGramms", "canonicalUrl", "64BitK3SimHashOneGramms", "64BitK3SimHashThreeAndFiveGramms")); 
		DeduplicateTrecRunFile rf = new DeduplicateTrecRunFile(1, cw09DocResolver, sim);
		
		return rf.deduplicate(runFileContent)
			.stream()
			.sorted()
			.collect(Collectors.toList());
	}
}
