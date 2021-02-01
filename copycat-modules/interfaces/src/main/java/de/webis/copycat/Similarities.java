package de.webis.copycat;

import java.util.Map;

public interface Similarities {
	Map<String, Float> calculateSimilarities(DocumentPair docPair);
}
