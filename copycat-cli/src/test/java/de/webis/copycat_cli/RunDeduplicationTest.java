package de.webis.copycat_cli;

import java.nio.file.Files;
import java.nio.file.Path;

import org.approvaltests.Approvals;
import org.junit.Test;

import lombok.SneakyThrows;

public class RunDeduplicationTest {

	@Test
	public void testExceptionIsThrownOnRunWitDuplicatedIds() {
		Approvals.verify(argsForInputFile("run-with-duplicated-ids.txt"));
	}
	
	@Test
	public void approveRunWithoutDuplicatesIsNotChanged() {
		Approvals.verify(argsForInputFile("run-without-duplicates.txt"));
	}

	@Test
	public void approveRunWithDuplicatedTopics() {
		Approvals.verify(argsForInputFile("run-with-duplicated-topics.txt"));
	}
	
	@Test
	public void approveRunWithSingleDuplicate() {
		Approvals.verify(argsForInputFile("run-with-single-duplicate.txt"));
	}
	
	@SneakyThrows
	private static String argsForInputFile(String inputFile) {
		Path outputFile = Files.createTempFile("unit-test-copycat", ".jsonl");
		
		String[] args = new String[] {
			"--outputRunFile", outputFile.toString(),
			"--inputRunFile", "src/test/resources/run-deduplication-test/" + inputFile,
			"--nearDuplicates", "src/test/resources/run-deduplication-test/task1-equivalence-classes-of-near-duplicates-top25.jsonl"
		};
		
		RunDeduplicationApp.deduplicateRun(args);
		String ret = new String(Files.readAllBytes(outputFile));
		outputFile.toFile().delete();
		
		return ret;
	}
}
