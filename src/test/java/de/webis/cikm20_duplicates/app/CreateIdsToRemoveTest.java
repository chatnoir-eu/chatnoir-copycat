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
}
