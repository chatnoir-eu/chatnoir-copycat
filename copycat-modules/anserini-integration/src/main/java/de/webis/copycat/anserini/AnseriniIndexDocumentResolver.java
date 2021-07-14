package de.webis.copycat.anserini;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import lombok.SneakyThrows;

public class AnseriniIndexDocumentResolver {
	
	public static void main(String[] args) throws Exception {
		//System.out.println(IOUtils.toString(AnseriniIndexDocumentResolver.class.getResourceAsStream("/META-INF/services/org.apache.lucene.codecs.Codec")));
		System.out.println("Classloader: " + AnseriniIndexDocumentResolver.class.getClassLoader());
		System.out.println(Codec.getDefault());
		org.apache.lucene.store.Directory a = null;
		org.apache.lucene.search.Query b = null;
		System.out.println(a + "<-->" + b);
		System.out.println("aaa->" + org.apache.lucene.codecs.lucene80.Lucene80Codec.class.getName());
		AnseriniIndexDocumentResolver docResolver = new AnseriniIndexDocumentResolver("src/test/resources/example-anserini-index/");
		System.out.println(docResolver);
		System.out.println(docResolver.loadDocumentContent("doc2"));
	}
	
	private final IndexSearcher searcher;
	
	public AnseriniIndexDocumentResolver(String indexDir) {
		try {
			IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)));
			searcher = new IndexSearcher(indexReader);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@SneakyThrows
	public Stream<Document> allDocumentsInIndex() {
		List<Integer> ret = new ArrayList<>();

		IndexReader reader = searcher.getIndexReader();
		for (int i=0; i<reader.maxDoc(); i++) {
		    ret.add(i);
		}
		
		return ret.stream().map(i -> {try {
			return reader.document(i);
		} catch(Exception e) {throw new RuntimeException(e);}});
	}
	
	public String loadDocumentContent(String id) {
		try {
			BooleanQuery queryForDoc = new BooleanQuery.Builder()
					.add(idIs(id), org.apache.lucene.search.BooleanClause.Occur.FILTER)
					.build();
			TopDocs docs = searcher.search(queryForDoc, 10);
			if(docs.scoreDocs.length != 1 ) {
				throw new RuntimeException("Fix this: " + id + " should be unique");
			}
			
			return doc(searcher.getIndexReader().document(docs.scoreDocs[0].doc));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public String doc(Document document) {
		String content = document.get("raw");
		if(content == null || content.trim().isEmpty()) {
			content = document.get("contents");
		}
		
		if(content == null || content.trim().isEmpty()) {
			throw new RuntimeException("Could not extract content for " + document);
		}
		
		return content;
	}
	
	public String id(Document document) {
		return document.get("id");
	}

	private static Query idIs(String documentId) {
		return new TermQuery(new Term("id", documentId));
	}
}
