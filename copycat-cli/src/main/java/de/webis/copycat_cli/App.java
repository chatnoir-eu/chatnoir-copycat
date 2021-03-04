package de.webis.copycat_cli;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import de.webis.cikm20_duplicates.app.ArgumentParsingUtil;
import de.webis.cikm20_duplicates.app.DeduplicateTrecRunFile;
import de.webis.cikm20_duplicates.app.DeduplicateTrecRunFile.DefaultSimilarityCalculation;
import de.webis.copycat.DocumentPreprocessing;
import de.webis.copycat.DocumentResolver;
import de.webis.copycat.Similarities;
import de.webis.copycat.document_preprocessing.CopyCatPreprocessing;
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
		
		if(outputFile.exists()) {
			System.out.println("The specified " + ArgumentParsingUtil.ARG_OUTPUT + " '" + outputFile + "' exists.\nSkip...");
			return;
		}
		
		Similarities sim = new DefaultSimilarityCalculation(parsedArgs.getList(ARG_SIMILARITIES));
		
		Path inputPath = Paths.get(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
		InputStream runFileContent = RunLine.openRunFile(inputPath);

		DeduplicateTrecRunFile dedup = new DeduplicateTrecRunFile(
			parsedArgs.getInt(ARG_THREADS),docResolver, sim, 
			parsedArgs.getDouble(ARG_S3_THRESHOLD), parsedArgs.getInt(ARG_RANKS),
			parsedArgs.getBoolean(ARG_RUN_FILE), !parsedArgs.getBoolean(ARG_RUN_FILE)
			
		);
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
			dedup.deduplicate(runFileContent).forEach(i -> {
				try {
					writer.write(i +"\n");
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		}
	}
}
