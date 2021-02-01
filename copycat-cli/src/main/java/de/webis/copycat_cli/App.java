package de.webis.copycat_cli;

import de.webis.copycat_cli.doc_resolver.AnseriniDocumentResolver;
import de.webis.copycat_cli.doc_resolver.ChatNoirDocumentResolver;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class App {
	public static void main(String[] args) {
		testOnLocalAnserini();
		testOnCw();
	}

	private static void testOnLocalAnserini() {
		String id = "doc1";
		System.out.println("Retrieve " + id);
		String content = new AnseriniDocumentResolver("src/test/resources/example-anserini-index/").loadCollectionDocument(id).getContent();

		System.out.println(content);
	}
	
	private static void testOnCw() {
		try {
			String id = "clueweb09-en0002-17-16080";
			System.out.println("Retrieve " + id);
			CollectionDocument doc = new ChatNoirDocumentResolver().loadCollectionDocument(id);
		
			System.out.println(doc.getContent());
			System.out.println(doc.getCrawlingTimestamp());
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
