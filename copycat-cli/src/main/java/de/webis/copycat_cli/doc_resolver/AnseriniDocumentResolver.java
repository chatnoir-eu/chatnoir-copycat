package de.webis.copycat_cli.doc_resolver;

import de.webis.copycat.DocumentPreprocessing;
import de.webis.copycat.DocumentResolver;
import de.webis.copycat.anserini.AnseriniIndexDocumentResolver;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class AnseriniDocumentResolver implements DocumentResolver {

	private final AnseriniIndexDocumentResolver internalResolver;
	
	private DocumentPreprocessing preprocessing;
	
	public AnseriniDocumentResolver(String indexDir) {
		internalResolver = new AnseriniIndexDocumentResolver(indexDir);
	}
	
	@Override
	public CollectionDocument loadCollectionDocument(String id) {
		String content = internalResolver.loadDocumentContent(id);
		
		if(preprocessing != null) {
			content = preprocessing.preprocessRawDocument(content);
		}
		
		return CollectionDocument.collectionDocument(content, id);
	}
	

	public void configure(DocumentPreprocessing preprocessing) {
		this.preprocessing = preprocessing;
	}
}
