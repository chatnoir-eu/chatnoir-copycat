package de.webis.copycat.document_preprocessing;

import de.webis.copycat.DocumentPreprocessing;
import io.anserini.index.transform.StringTransform;
import lombok.SneakyThrows;

/**
 * All of Anserini's String transformations (and classes that implement this interface)
 * that transform raw documents into text (like html to plain text)
 *  are supported via this class by reflection.
 * 
 * @author Maik Fröbe
 *
 */
class AnseriniDocumentTransformation implements DocumentPreprocessing {
	private StringTransform anseriniTransformer;

	AnseriniDocumentTransformation(String transformerName) {
		this(stringTransformClass(transformerName));
	}
	
	@SneakyThrows
	public AnseriniDocumentTransformation(Class<? extends StringTransform> clazz) {
		anseriniTransformer = clazz.newInstance();
	}
	
	@Override
	public String preprocessRawDocument(String text) {
		return anseriniTransformer.apply(text);
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static Class<? extends StringTransform> stringTransformClass(String transformerName) {
		return (Class<? extends StringTransform>) Class.forName(StringTransform.class.getPackage().getName() + "." + transformerName);
	}
}
