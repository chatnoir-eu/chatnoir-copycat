package de.webis.copycat_cli;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codehaus.jackson.map.ObjectMapper;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.DefaultUndirectedGraph;

import com.google.common.io.Files;

import de.webis.copycat.DocumentPreprocessing;
import de.webis.copycat.DocumentResolver;
import de.webis.copycat.Similarities;
import de.webis.copycat.document_preprocessing.CopyCatPreprocessing;
import de.webis.copycat_spark.app.ArgumentParsingUtil;
import de.webis.copycat_spark.app.DeduplicateTrecRunFile;
import de.webis.copycat_spark.app.DeduplicateTrecRunFile.DefaultSimilarityCalculation;
import de.webis.copycat_spark.app.ExtractDocumentsToJudge;
import de.webis.trec_ndd.spark.DocumentGroup;
import de.webis.trec_ndd.spark.RunLine;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.Data;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.inf.Namespace;

@Data
public class App implements CliArguments {

	@SneakyThrows
	public static void main(String[] args) {
		Namespace parsedArgs = CliArguments.parseArgs(args);
		if(parsedArgs == null) {
			return;
		}
		
		DocumentPreprocessing documentPreprocessing = CopyCatPreprocessing.documentPreprocessing(parsedArgs);
		DocumentResolver docResolver = CliArguments.docResolver(parsedArgs);
		docResolver.configure(documentPreprocessing);
		
		if(parsedArgs.getString(CliArguments.ARG_RETRIEVE_DOC) != null) {
			CollectionDocument doc = docResolver.loadCollectionDocument(parsedArgs.getString(CliArguments.ARG_RETRIEVE_DOC));
			System.out.println(doc == null ? "": doc.getFullyCanonicalizedContent());
			return;
		}
		
		File outputFile = new File(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT));
		Similarities sim = new DefaultSimilarityCalculation(parsedArgs.getList(ARG_SIMILARITIES));
		
		Path inputPath = Paths.get(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
		InputStream runFileContent = RunLine.openRunFile(inputPath);

		DeduplicateTrecRunFile dedup = new DeduplicateTrecRunFile(
			parsedArgs.getInt(ARG_THREADS), docResolver, sim, 
			parsedArgs.getDouble(ARG_S3_THRESHOLD), parsedArgs.getInt(ARG_RANKS),
			parsedArgs.getBoolean(ARG_RUN_FILE), !parsedArgs.getBoolean(ARG_RUN_FILE)
			
		);
		
		if(outputFile.exists()) {
			System.out.println("The specified " + ArgumentParsingUtil.ARG_OUTPUT + " '" + outputFile + "' exists.\nSkip...");
			
			persistExamples(parsedArgs, dedup);
			persistGroupsOfEquivalentDocuments(parsedArgs);
			return;
		}

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
			dedup.deduplicate(runFileContent).forEach(i -> {
				try {
					writer.write(i +"\n");
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		}
		
		persistExamples(parsedArgs, dedup);
		persistGroupsOfEquivalentDocuments(parsedArgs);
	}

	@SneakyThrows
	private static void persistExamples(Namespace parsedArgs, DeduplicateTrecRunFile dedup) {
		if(parsedArgs.getString(CliArguments.ARG_PERSIST_EXAMPLES) == null) {
			return;
		}
		
		Path outputPath = Paths.get(parsedArgs.getString(CliArguments.ARG_PERSIST_EXAMPLES));
		for(String l: Files.readLines(Paths.get(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT)).toFile(), StandardCharsets.UTF_8)) {
			new ExtractDocumentsToJudge(dedup).run(l, outputPath);
		}
	}
	
	@SneakyThrows
	static void persistGroupsOfEquivalentDocuments(Namespace parsedArgs) {
		if(parsedArgs.getString(CliArguments.ARG_DOC_CLASSES_FILE) == null) {
			return;
		}
		
		List<String> in = Files.readLines(Paths.get(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT)).toFile(), StandardCharsets.UTF_8);
		Path out = Paths.get(parsedArgs.getString(CliArguments.ARG_DOC_CLASSES_FILE));
		String ret = "";
		for(String jsonlInput: in) {
			List<String> tmp = extractGroupsOfEquivalentDocuments(jsonlInput);
			ret += "\n" + tmp.stream().collect(Collectors.joining("\n"));
			ret = ret.trim();
		}
		
		Files.write(ret.getBytes(), out.toFile());
	}

	@SneakyThrows
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static List<String> extractGroupsOfEquivalentDocuments(String jsonlInput) {
		Map<String, Object> parsed = new ObjectMapper().readValue(jsonlInput, Map.class);
		return extractGroupsOfEquivalentDocuments((List) parsed.get("similarities"));
	}
	
	private static List<String> extractGroupsOfEquivalentDocuments(List<Map<String, Object>> similarities) {
		DefaultUndirectedGraph<String, Integer> graph = new DefaultUndirectedGraph<>(Integer.class);
		int id = 0;
		for(Map<String, Object> pair: similarities) {
			String firstId = (String) pair.get("firstId");
			String secondId = (String) pair.get("secondId");
			
			graph.addVertex(firstId);
			graph.addVertex(secondId);
			graph.addEdge(firstId, secondId, ++id);
		}

		return extractGroupsOfEquivalentDocuments(new ConnectivityInspector<>(graph));
	}

	private static List<String> extractGroupsOfEquivalentDocuments(ConnectivityInspector<String, Integer> connectivityInspector) {
		List<String> ret = new ArrayList<>();
		for(Set<String> equivalentDocs: connectivityInspector.connectedSets()) {
			if(equivalentDocs.size() < 2) {
				throw new RuntimeException("Could not handle: " + equivalentDocs);
			}
			ArrayList<String> ids = new ArrayList<>(equivalentDocs);
			Collections.sort(ids);
			
			DocumentGroup docGroup = new DocumentGroup();
			docGroup.setIds(ids);
			docGroup.setHash("" + ids.toString().hashCode());
			
			ret.add(docGroup.toString());
		}
		
		return ret;
	}
}