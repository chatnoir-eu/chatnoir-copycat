package de.webis.copycat.document_preprocessing;

import java.io.Serializable;

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
@SuppressWarnings("serial")
public class AnseriniDocumentTransformation implements DocumentPreprocessing {
	private final SerializableStringTransform anseriniTransformer;

	AnseriniDocumentTransformation(String transformerName) {
		this(stringTransformClass(transformerName));
	}
	
	public AnseriniDocumentTransformation(Class<? extends StringTransform> clazz) {
		anseriniTransformer = serializableInstance(clazz);
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
	
	@SneakyThrows
	private static SerializableStringTransform serializableInstance(Class<? extends StringTransform> clazz) {
		StringTransform anseriniTransformer = clazz.newInstance();
		
		return new SerializableStringTransform() {
			@Override
			public String apply(String t) {
				return anseriniTransformer.apply(t);
			}
		};
	}
	
	static abstract class SerializableStringTransform extends StringTransform implements Serializable {}
}
