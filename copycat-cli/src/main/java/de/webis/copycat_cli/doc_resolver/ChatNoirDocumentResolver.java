package de.webis.copycat_cli.doc_resolver;

import de.webis.copycat.DocumentPreprocessing;
import de.webis.copycat.DocumentResolver;
import de.webis.copycat_spark.util.CollectionDocumentUtil.HdfsMapFileDocumentResolver;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class ChatNoirDocumentResolver implements DocumentResolver {
	
	private DocumentResolver internalResolver;

	@Override
	public CollectionDocument loadCollectionDocument(String id) {
		return internalResolver.loadCollectionDocument(id);
	}
	
	public void configure(DocumentPreprocessing config) {
		internalResolver = HdfsMapFileDocumentResolver.smartDocumentResolver(config);
	}
}
