package de.webis.copycat_cli;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import org.approvaltests.Approvals;
import org.junit.Test;

import lombok.SneakyThrows;
import net.sourceforge.argparse4j.inf.Namespace;

public class PersistingGroupsOfEquivalentDocumentsTest {

	@Test
	public void approveSmallSampleWithMultipleTopicsAndSmallGroups() {
		Approvals.verify(runOnFile("small-sample-multiple-topics-only-small-groups.jsonl"));
	}
	
	@Test
	public void approveSmallSampleWithMultipleTopicsAndLargeGroups() {
		Approvals.verify(runOnFile("multiple-topics-larger-groups.jsonl"));
	}
	
	@Test
	public void approveToucheExampleTopicTask1() {
		Approvals.verify(runOnFile("touche21-topic91.jsonl"));
	}
	
	@SneakyThrows
	String runOnFile(String inputFile) {
		Path outputFile = Files.createTempFile("unit-test-copycat", ".jsonl");
		Namespace args = argsForInputFile("src/test/resources/test-pairwise-near-duplicates/" +inputFile, outputFile.toFile());
		App.persistGroupsOfEquivalentDocuments(args);
		String ret = new String(Files.readAllBytes(outputFile));
		outputFile.toFile().delete();
		
		return ret;
	}
	
	private static Namespace argsForInputFile(String inputFile, File outputFile) {
		return CliArguments.parseArgs(new String[] {
			"--documents", "AnseriniIndex",
			"--output", inputFile,
			"--input", "a",
			"--documentEquivalenceClassFile", outputFile.toString()
		});
	}
}
