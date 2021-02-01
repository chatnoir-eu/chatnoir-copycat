package de.webis.cikm20_duplicates.app;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.app.DeduplicateTrecRunFile.DefaultSimilarityCalculation;
import de.webis.copycat.DocumentResolver;

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
			"{\"topic\":\"1\",\"similarities\":[{\"firstId\":\"clueweb09-en0058-20-30937\",\"secondId\":\"clueweb09-en0060-58-25055\",\"similarities\":{\"s3\":0.875,\"cosine(1-grams)\":0.9868972,\"cosine(8-grams)\":0.8750517,\"cosine(3+5-grams)\":0.92026484,\"url\":0.0,\"simhash(1-grams)\":0.953125,\"simhash(3+5-grams)\":0.96875,\"md5\":0.0,\"text-profile\":0.0}}],\"docs\":2}"
		);

		List<String> actual = deduplicate(runFileContent);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testOnRunFileWithMultipleDuplicatesWithinSingleTopic() {
		String runFileContent = "1 Q0 clueweb09-en0060-58-25055 2 0.0 tag\n"
				+ "1 Q0 clueweb09-en0058-20-30937 2 0.0 tag\n"
				+ "1 Q0 clueweb09-en0019-66-11581 2 0.0 tag\n";
		List<String> expected = Arrays.asList(
				"{\"topic\":\"1\",\"similarities\":[{\"firstId\":\"clueweb09-en0058-20-30937\",\"secondId\":\"clueweb09-en0060-58-25055\",\"similarities\":{\"s3\":0.875,\"cosine(1-grams)\":0.9868972,\"cosine(8-grams)\":0.8750517,\"cosine(3+5-grams)\":0.92026484,\"url\":0.0,\"simhash(1-grams)\":0.953125,\"simhash(3+5-grams)\":0.96875,\"md5\":0.0,\"text-profile\":0.0}}],\"docs\":3}");

		List<String> actual = deduplicate(runFileContent);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testOnRunFileWithMultipleTopics() {
		String runFileContent = "1 Q0 clueweb09-en0060-58-25055 2 0.0 tag\n"
				+ "2 Q0 clueweb09-en0019-66-11581 2 0.0 tag\n" + "1 Q0 clueweb09-en0058-20-30937 2 0.0 tag\n";
		List<String> expected = Arrays.asList(
				"{\"topic\":\"1\",\"similarities\":[{\"firstId\":\"clueweb09-en0058-20-30937\",\"secondId\":\"clueweb09-en0060-58-25055\",\"similarities\":{\"s3\":0.875,\"cosine(1-grams)\":0.9868972,\"cosine(8-grams)\":0.8750517,\"cosine(3+5-grams)\":0.92026484,\"url\":0.0,\"simhash(1-grams)\":0.953125,\"simhash(3+5-grams)\":0.96875,\"md5\":0.0,\"text-profile\":0.0}}],\"docs\":2}",
				"{\"topic\":\"2\",\"similarities\":[],\"docs\":1}");

		List<String> actual = deduplicate(runFileContent);

		Assert.assertEquals(expected, actual);
	}

	private List<String> deduplicate(String runFileContent) {
		DocumentResolver cw09DocResolver = EnrichSimHashNearDuplicatesWithS3SimilarityIntegrationTest.cw09Resolver().get();
		DefaultSimilarityCalculation sim = new DefaultSimilarityCalculation(Arrays.asList(
			"s3", "cosine(1-grams)", "cosine(8-grams)", "cosine(3+5-grams)",
			"url", "simhash(1-grams)", "simhash(3+5-grams)",
			"md5", "text-profile"
		)); 
		DeduplicateTrecRunFile rf = new DeduplicateTrecRunFile(1, cw09DocResolver, sim, 0.6, 5);
		
		return rf.deduplicate(runFileContent)
			.sorted()
			.collect(Collectors.toList());
	}
}
