package de.webis.copycat.document_preprocessing;

import de.webis.copycat.DocumentPreprocessing;

@SuppressWarnings("serial")
public class AnseriniDocumentPreprocessing implements DocumentPreprocessing {

	private final AnseriniDocumentTransformation defaultAnseriniTransformation = new AnseriniDocumentTransformation("JsoupStringTransform"); 
	
	@Override
	public String preprocessRawDocument(String text) {
		return defaultAnseriniTransformation.preprocessRawDocument(text);
	}
}
