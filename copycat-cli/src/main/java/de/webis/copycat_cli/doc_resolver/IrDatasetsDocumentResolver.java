package de.webis.copycat_cli.doc_resolver;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

import org.codehaus.jackson.map.ObjectMapper;

import de.webis.copycat_spark.app.ArgumentParsingUtil;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.Data;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

@Data
public class IrDatasetsDocumentResolver {
	
	private final String path;
	
	@SneakyThrows
	public static void main(String[] args) {
		Namespace parsedArgs = parseArgs(args);
		if(parsedArgs == null) {
			return;
		}
		Stream<CollectionDocument> input = new IrDatasetsDocumentResolver(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT))
					.allDocuments();
		
		PrintWriter out = new PrintWriter(new FileWriter(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT)));
		
		input.forEach(i -> {
			out.write(i.toString() + "\n");
		});
	}
	
	@SneakyThrows
	public Stream<CollectionDocument> allDocuments() {
		return Files.lines(Paths.get(path))
				.map(i -> toCollectionDocument(i));
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private CollectionDocument toCollectionDocument(String json) {
		Map<String, String> parsedJson = new ObjectMapper().readValue(json, Map.class);
		
		return CollectionDocument.collectionDocument(
			parsedJson.get("contents"),
			parsedJson.get("id")
		);
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
		ArgumentParser ret = ArgumentParsers.newFor("CopyCat: Transform corpus extracted with ir_datasets to jsonl.")
				.build();
		
		ret.addArgument("--" + ArgumentParsingUtil.ARG_INPUT)
			.help("The ir_datasets corpus that should be transformed (e.g., extracted with `ir_datasets export msmarco-document docs --format jsonl| jq -c '{\"id\": .doc_id, \"contents\": .title}' > file`).")
			.required(true);
		
		ret.addArgument("--" + ArgumentParsingUtil.ARG_OUTPUT)
			.help("The resulting documents are stored here in jsonl format.")
			.required(true);
		
		return ret;
	}
}
