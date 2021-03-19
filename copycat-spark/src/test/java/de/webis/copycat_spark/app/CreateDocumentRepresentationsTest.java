package de.webis.copycat_spark.app;

import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat.DocumentPreprocessing;
import de.webis.copycat.document_preprocessing.CopyCatPreprocessing;
import de.webis.copycat_spark.spark.SparkIntegrationTestBase;
import net.sourceforge.argparse4j.inf.Namespace;

public class CreateDocumentRepresentationsTest extends SparkIntegrationTestBase {
	@Test
	public void approveDocumentToTextTransformationFromArgsWithoutArgument() {
		DocumentPreprocessing actual = transformation(
			"-i", "foo-bar",
			"-o", "foo-bar",
			"-f", "CLUEWEB09"
		);

		Assert.assertEquals("class de.webis.copycat.document_preprocessing.StemmingAndStopWordRemovalDocumentPreprocessing", actual.getClass().toString());
	}

	@Test(expected = RuntimeException.class)
	public void approveDocumentToTextTransformationForWrongArguments() {
		DocumentPreprocessing actual = transformation(
			"-i", "foo-bar",
			"-o", "foo-bar",
			"-f", "CLUEWEB09",
			"--mainsContentExtraction", "false"
		);
		
		Assert.assertNull(actual);
	}
	
	@Test
	public void approveDocumentToTextTransformationFromArgsWithoutArgument2() {
		DocumentPreprocessing actual = transformation(
			"-i", "foo-bar",
			"-o", "foo-bar",
			"-f", "CLUEWEB09"
		);
		

		Assert.assertEquals("class de.webis.copycat.document_preprocessing.StemmingAndStopWordRemovalDocumentPreprocessing", actual.getClass().toString());
	}
	
	@Test
	public void approveDocumentToTextTransformationFromArgsWithoutArgument3() {
		DocumentPreprocessing actual = transformation(
			"-i", "foo-bar",
			"-o", "foo-bar",
			"-f", "CLUEWEB09",
			"--contentExtraction", "Boilerpipe"
		);
		
		Assert.assertEquals("class de.webis.copycat.document_preprocessing.StemmingAndStopWordRemovalDocumentPreprocessing", actual.getClass().toString());
	}
	
	private static DocumentPreprocessing transformation(String...args) {
		Namespace parsedArgs = CreateDocumentRepresentations.validArgumentsOrNull(args);
		
		return CopyCatPreprocessing.documentPreprocessing(parsedArgs);
	}
}
