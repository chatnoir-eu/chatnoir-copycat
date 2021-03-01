package de.webis.copycat.document_preprocessing;

import java.nio.file.Files;
import java.nio.file.Paths;

import de.webis.copycat.DocumentPreprocessing;
import lombok.Data;
import lombok.SneakyThrows;

@Data
public class DocumentPreprocessingTest {
	
	private final DocumentPreprocessing preprocessing;
	
	public DocumentPreprocessingTest(DocumentPreprocessing preprocessing) {
		this.preprocessing = new StemmingAndStopWordRemovalDocumentPreprocessing(preprocessing, "porter", true);
	}
	
	public String preprocessedHtml(String fileName) {
		return preprocessing.preprocessRawDocument(html(fileName));
	}
	
	@SneakyThrows
	private static String html(String fileName) {
		return new String(Files.readAllBytes(Paths.get("src/test/resources/example-html-" + fileName + ".html")));
	}
}
