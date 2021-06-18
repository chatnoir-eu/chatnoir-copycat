package de.webis.copycat.document_preprocessing;

import org.jsoup.Jsoup;

import de.webis.copycat.DocumentPreprocessing;

/**
 * All of Anserini's String transformations (and classes that implement this interface)
 * that transform raw documents into text (like html to plain text)
 *  are supported via this class by reflection.
 * 
 * @author Maik Fröbe
 *
 */
@SuppressWarnings("serial")
public class AnseriniDocumentTransformation implements DocumentPreprocessing {
	AnseriniDocumentTransformation(String transformerName) {
		if(!"JsoupStringTransform".equals(transformerName)) {
			throw new RuntimeException("FIXME: JsoupStringTransform is the only supported AnseriniDocumentTransformation at the moment. Got: " + transformerName);
		}
	}
	
	@Override
	public String preprocessRawDocument(String text) {
		return Jsoup.parse(text).text();
	}
}
