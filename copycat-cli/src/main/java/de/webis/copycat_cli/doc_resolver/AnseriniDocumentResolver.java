package de.webis.copycat_cli.doc_resolver;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.webis.copycat.DocumentPreprocessing;
import de.webis.copycat.DocumentResolver;
import de.webis.copycat.anserini.AnseriniIndexDocumentResolver;
import de.webis.copycat_spark.app.ArgumentParsingUtil;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class AnseriniDocumentResolver implements DocumentResolver {

	private final AnseriniIndexDocumentResolver internalResolver;
	
	private DocumentPreprocessing preprocessing;
	
	@SneakyThrows
	public static void main(String[] args) {
		Namespace parsedArgs = parseArgs(args);
		if(parsedArgs == null) {
			return;
		}
		
		List<String> output = new ArrayList<>();
		List<CollectionDocument> input = new AnseriniDocumentResolver(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT))
					.allDocumentsInIndex().collect(Collectors.toList());
		
		for(int i=0; i< input.size(); i++) {
			output.add(input.get(i).toString());
			
			if(i%1000 == 0) {
				System.out.println("Processed Documents: " + i);
			}
		}
		
		Files.write(Paths.get(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT)), output);
	}
	
	static Namespace parseArgs(String[] args) {
		ArgumentParser parser = argParser();
		
		try {
			return parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			return null;
		}
	}

	static ArgumentParser argParser() {
		ArgumentParser ret = ArgumentParsers.newFor("CopyCat: Transform Anserini Index to jsonl.")
				.build();
		
		ret.addArgument("--" + ArgumentParsingUtil.ARG_INPUT)
			.help("The anserini index that should be transformed.")
			.required(true);
		
		ret.addArgument("--" + ArgumentParsingUtil.ARG_OUTPUT)
			.help("The resulting documents are stored here in jsonl format.")
			.required(true);
		
		return ret;
	}
	
	public AnseriniDocumentResolver(String indexDir) {
		internalResolver = new AnseriniIndexDocumentResolver(indexDir);
	}
	
	@Override
	public CollectionDocument loadCollectionDocument(String id) {
		String rawContent = internalResolver.loadDocumentContent(id);
		
		return toCollectionDocument(id, rawContent);
	}
	
	private CollectionDocument toCollectionDocument(String id, String content) {
		if(preprocessing != null) {
			content = preprocessing.preprocessRawDocument(content);
		}
		
		return CollectionDocument.collectionDocument(content, id);
	}
	
	public Stream<CollectionDocument> allDocumentsInIndex() {
		return internalResolver.allDocumentsInIndex()
			.map(i -> toCollectionDocument(internalResolver.id(i), internalResolver.doc(i)));
	}

	public void configure(DocumentPreprocessing preprocessing) {
		this.preprocessing = preprocessing;
	}
}
