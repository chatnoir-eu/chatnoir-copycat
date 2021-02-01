package de.webis.copycat_cli.doc_resolver;

import de.webis.copycat.DocumentResolver;
import de.webis.copycat.anserini.AnseriniIndexDocumentResolver;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class AnseriniDocumentResolver implements DocumentResolver {

	private final AnseriniIndexDocumentResolver internalResolver;
	
	public AnseriniDocumentResolver(String indexDir) {
		internalResolver = new AnseriniIndexDocumentResolver(indexDir);
	}
	
	@Override
	public CollectionDocument loadCollectionDocument(String id) {
		String content = internalResolver.loadDocumentContent(id);
		
		return CollectionDocument.collectionDocument(content, id);
	}
}
