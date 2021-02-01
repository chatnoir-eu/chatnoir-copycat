package de.webis.copycat_cli.doc_resolver;

import de.webis.cikm20_duplicates.util.CollectionDocumentUtil.HdfsMapFileDocumentResolver;
import de.webis.copycat.DocumentResolver;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class ChatNoirDocumentResolver implements DocumentResolver {
	
	private final DocumentResolver internalResolver = HdfsMapFileDocumentResolver.smartDocumentResolver();

	@Override
	public CollectionDocument loadCollectionDocument(String id) {
		return internalResolver.loadCollectionDocument(id);
	}
}
