package de.webis.copycat;

import de.webis.trec_ndd.trec_collections.CollectionDocument;

public interface DocumentResolver {
	public CollectionDocument loadCollectionDocument(String id);
	
	public default void configure(DocumentPreprocessing config) {
		throw new RuntimeException("This method must be implemented by implementint interfaces.");
	}
}
