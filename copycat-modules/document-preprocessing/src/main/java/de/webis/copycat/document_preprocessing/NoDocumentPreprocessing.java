package de.webis.copycat.document_preprocessing;

import de.webis.copycat.DocumentPreprocessing;

/**
 * This preprocessing of documents simply returns the text without any changes.
 * This is useful in cases that documents are already preprocessed, e.g., when they come from an Index.
 * 
 * @author Maik Fröbe
 *
 */
class NoDocumentPreprocessing implements DocumentPreprocessing {
	@Override
	public String preprocessRawDocument(String text) {
		return text;
	}
}
