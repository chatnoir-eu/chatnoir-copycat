package de.webis.cikm20_duplicates.app;

import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.app.CreateIdsToRemove.IdsToRemoveConfiguration;

import static de.webis.cikm20_duplicates.app.CreateIdsToRemove.parseArgs;

import java.util.Arrays;

public class CreateIdsToRemoveTest {
	@Test(expected=Exception.class)
	public void testThatWrongNumberOfInputOutputPathsFails() {
		parseArgs("--exactDuplicateInputs", "a", "b", "--nearDuplicateInputs", "a", "b", "-o", "a", "--keepIds", "CW12");
	}
	
	@Test(expected=Exception.class)
	public void testThatRedundantInputPathsAreInvalid() {
		parseArgs("--exactDuplicateInputs", "a", "--nearDuplicateInputs", "a", "-o", "b", "c", "--keepIds", "CW12");
	}
	
	@Test
	public void testWithValidInput() {
		IdsToRemoveConfiguration expected = new IdsToRemoveConfiguration(
			Arrays.asList("a"),
			Arrays.asList("b"),
			Arrays.asList("c", "d"),
			"CW12"
		);
		IdsToRemoveConfiguration actual = parseArgs("--exactDuplicateInputs", "a", "--nearDuplicateInputs", "b", "-o", "c", "d", "--keepIds", "CW12");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void t() {
		IdsToRemoveConfiguration expected = new IdsToRemoveConfiguration(
				Arrays.asList("s3a://corpus-copycat/exact-duplicates/{cw09-cw12-cc-2015-11,cc-2015-11,cw09,cw12}/", "s3a://corpus-copycat/canonical-url-groups/simhash-one-grams-cw09-cw12-cc15-exact-duplicates/url-simhash-one-grams-cw09-cw12-cc15-exact-duplicates/"),
				Arrays.asList("s3a://corpus-copycat/near-duplicates/cw09-cw12-cc-2015-11/", "s3a://corpus-copycat/canonical-url-groups/near-duplicates-simhash-one-grams-cw09-cw12-cc15/"),
				Arrays.asList("sigir21/docs-to-remove/cw09-cw12-cc15/intermediate/simhash-3-5-grams-docs-to-remove", "sigir21/docs-to-remove/cw09-cw12-cc15/intermediate/url-groups-docs-to-remove", "sigir21/docs-to-remove/cw09-cw12-cc15/docs-to-remove"),
				"ALL"
			);
		IdsToRemoveConfiguration actual = parseArgs(
			"--exactDuplicateInputs", "s3a://corpus-copycat/exact-duplicates/{cw09-cw12-cc-2015-11,cc-2015-11,cw09,cw12}/", "s3a://corpus-copycat/canonical-url-groups/simhash-one-grams-cw09-cw12-cc15-exact-duplicates/url-simhash-one-grams-cw09-cw12-cc15-exact-duplicates/",
			"--nearDuplicateInputs", "s3a://corpus-copycat/near-duplicates/cw09-cw12-cc-2015-11/", "s3a://corpus-copycat/canonical-url-groups/near-duplicates-simhash-one-grams-cw09-cw12-cc15/",
			"--output", "sigir21/docs-to-remove/cw09-cw12-cc15/intermediate/simhash-3-5-grams-docs-to-remove", "sigir21/docs-to-remove/cw09-cw12-cc15/intermediate/url-groups-docs-to-remove", "sigir21/docs-to-remove/cw09-cw12-cc15/docs-to-remove",
			"--keepIds", "ALL"
		);

		Assert.assertEquals(expected, actual);
	}
}
