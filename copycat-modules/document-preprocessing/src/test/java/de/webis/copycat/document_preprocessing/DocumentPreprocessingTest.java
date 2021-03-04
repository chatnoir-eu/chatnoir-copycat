package de.webis.copycat.document_preprocessing;

import java.nio.file.Files;
import java.nio.file.Paths;

import de.webis.copycat.DocumentPreprocessing;
import lombok.Data;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

@Data
public class DocumentPreprocessingTest {
	
	private final DocumentPreprocessing preprocessing;
	
	public DocumentPreprocessingTest(String...args) {
		Namespace parsedArgs = args(args);
		
		this.preprocessing = CopyCatPreprocessing.documentPreprocessing(parsedArgs);
	}
	
	public String preprocessedHtml(String fileName) {
		return preprocessing.preprocessRawDocument(html(fileName));
	}
	
	@SneakyThrows
	private static String html(String fileName) {
		return new String(Files.readAllBytes(Paths.get("src/test/resources/example-html-" + fileName + ".html")));
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
