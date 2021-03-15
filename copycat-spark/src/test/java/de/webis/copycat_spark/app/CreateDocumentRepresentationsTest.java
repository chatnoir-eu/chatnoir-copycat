package de.webis.copycat_spark.app;

import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.app.CreateDocumentRepresentations;
import de.webis.copycat_spark.app.CreateDocumentRepresentations.DocumentToTextTransformation;
import de.webis.copycat_spark.spark.SparkIntegrationTestBase;
import net.sourceforge.argparse4j.inf.Namespace;

public class CreateDocumentRepresentationsTest extends SparkIntegrationTestBase {
	@Test
	public void approveDocumentToTextTransformationFromArgsWithoutArgument() {
		DocumentToTextTransformation actual = transformation(
			"-i", "foo-bar",
			"-o", "foo-bar",
			"-f", "CLUEWEB09"
		);
		
		Assert.assertNull(actual);
	}

	@Test(expected = RuntimeException.class)
	public void approveDocumentToTextTransformationForWrongArguments() {
		DocumentToTextTransformation actual = transformation(
			"-i", "foo-bar",
			"-o", "foo-bar",
			"-f", "CLUEWEB09",
			"--mainsContentExtraction", "false"
		);
		
		Assert.assertNull(actual);
	}
	
	@Test
	public void approveDocumentToTextTransformationFromArgsWithoutArgument2() {
		DocumentToTextTransformation actual = transformation(
			"-i", "foo-bar",
			"-o", "foo-bar",
			"-f", "CLUEWEB09",
			"--mainContentExtraction", "false"
		);
		
		Assert.assertNull(actual);
	}
	
	@Test
	public void approveDocumentToTextTransformationFromArgsWithoutArgument3() {
		DocumentToTextTransformation actual = transformation(
			"-i", "foo-bar",
			"-o", "foo-bar",
			"-f", "CLUEWEB09",
			"--mainContentExtraction", "true"
		);
		
		Assert.assertEquals("class de.webis.copycat_spark.app.CreateDocumentRepresentations$MainContentDocumentToTextTransformation", actual.getClass().toString());
	}
	
	private static DocumentToTextTransformation transformation(String...args) {
		Namespace parsedArgs = CreateDocumentRepresentations.validArgumentsOrNull(args);
		
		return CreateDocumentRepresentations.transformation(parsedArgs);
	}
}
