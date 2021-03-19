package de.webis.copycat.document_preprocessing;

import de.webis.copycat.DocumentPreprocessing;
import lombok.SneakyThrows;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

@SuppressWarnings("serial")
public class BoilerpipeDocumentPreprocessing implements DocumentPreprocessing {
	@Override
	@SneakyThrows
	public String preprocessRawDocument(String html) {
		return ArticleExtractor.getInstance().getText(html);
	}
}
