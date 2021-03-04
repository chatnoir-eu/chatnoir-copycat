package de.webis.copycat.document_preprocessing;

import org.junit.Assert;
import org.junit.Test;

import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

public class PreprocessingArgsTest {
	@Test
	public void testWithoutArgs() {
		Namespace args = args();
		PreprocessingArgs expected = new PreprocessingArgs();
		PreprocessingArgs actual = PreprocessingArgs.fromArgs(args);
		
		Assert.assertEquals(actual, expected);
	}
	
	@Test
	public void testWithNonDefaultArgs() {
		Namespace args = args("--keepStopwords", "True", "--stopwords", "test", "--contentExtraction", "Boilerpipe", "--stemmer", "krovetz");
		
		PreprocessingArgs expected = new PreprocessingArgs();
		expected.setContentExtraction("Boilerpipe");
		expected.setKeepStopwords(true);
		expected.setStemmer("krovetz");
		expected.setStopwords("test");
		
		PreprocessingArgs actual = PreprocessingArgs.fromArgs(args);
		
		Assert.assertEquals(actual, expected);
	}
	
	@SneakyThrows
	private static Namespace args(String...args) {
		ArgumentParser parser = parser();
		
		return parser.parseArgs(args);
	}
	
	private static ArgumentParser parser() {
		ArgumentParser ret = ArgumentParsers.newFor("")
				.build();
		
		PreprocessingArgs.addArgs(ret);
		
		return ret;
	}
}
