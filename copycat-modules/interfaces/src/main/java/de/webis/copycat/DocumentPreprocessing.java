package de.webis.copycat;

import java.io.Serializable;

public interface DocumentPreprocessing extends Serializable {
	public String preprocessRawDocument(String text);
}
