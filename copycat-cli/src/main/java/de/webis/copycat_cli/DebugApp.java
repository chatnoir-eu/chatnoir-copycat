package de.webis.copycat_cli;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;

import de.webis.copycat_cli.doc_resolver.AnseriniDocumentResolver;
import de.webis.copycat_cli.doc_resolver.ChatNoirDocumentResolver;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class DebugApp {
	public static void main(String[] args) {
		printregisteredHadoopFileSystemConfiguration();
		printHdfsSchemaClass();
		testOnLocalAnserini();
		testOnCw();
	}
	
	private static void printregisteredHadoopFileSystemConfiguration() {
		try {
			InputStream is = DebugApp.class.getResourceAsStream("/META-INF/services/org.apache.hadoop.fs.FileSystem");
			System.out.println(IOUtils.toString(is));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void printHdfsSchemaClass() {
		try {
			System.out.println("----> " + FileSystem.getFileSystemClass("hdfs", null));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
