package de.webis.copycat_cli;

import de.webis.copycat.DocumentResolver;
import de.webis.copycat.document_preprocessing.PreprocessingArgs;
import de.webis.copycat_cli.doc_resolver.AnseriniDocumentResolver;
import de.webis.copycat_cli.doc_resolver.ChatNoirDocumentResolver;
import de.webis.copycat_spark.app.ArgumentParsingUtil;
import de.webis.copycat_spark.app.DeduplicateTrecRunFile.DefaultSimilarityCalculation;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public interface CliArguments {
	public static final String ARG_DOC_RESOLVER = "documents";
	public static final String ARG_SIMILARITIES = "similarities";
	public static final String ARG_THREADS = "threads";
	public static final String ARG_S3_THRESHOLD = "s3Threshold";
	public static final String ARG_RANKS = "ranks";
	public static final String ARG_STRING_TRANSFORMATION = "stringTransformation";
	public static final String ARG_ANSERINI_INDEX = "anseriniIndex";
	public static final String ARG_RUN_FILE = "runFile";
	public static final String ARG_RETRIEVE_DOC = "retrieveDocId";
	
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
		ArgumentParser ret = ArgumentParsers.newFor("CopyCat: Deduplication of run files and qrels.")
				.build();
		
		ret.addArgument("--" + ArgumentParsingUtil.ARG_INPUT)
			.help("The run file or qrel file that should be deduplicated.")
			.required(true);
		
		ret.addArgument("--" + ArgumentParsingUtil.ARG_OUTPUT)
			.help("The result of the deduplication in jsonl format.")
			.required(true);
		
		ret.addArgument("--" + ARG_SIMILARITIES)
			.choices(DefaultSimilarityCalculation.PREDEFINED_SIMILARITIES.keySet())
			.help("Calculate all passed similarities.")
			.nargs("+");
		
		ret.addArgument("--" + ARG_DOC_RESOLVER)
			.choices("ChatNoirMapfiles", "AnseriniIndex")
			.help("Use the passed DocumentResolver to load the documents. E.g. AnseriniIndex loads documents by accessing a local anserini-index.")
			.required(true);
		
		ret.addArgument("--" + ARG_ANSERINI_INDEX)
			.help("When using AnseriniIndex as resolver for documents, we use the specified index.")
			.setDefault(".")
			.required(false);
	
		ret.addArgument("--" + ARG_RETRIEVE_DOC)
			.help("Retrieve a single document from and print it to the console. This is useful to check the preprocessing on a few example documents.")
			.setDefault((String) null)
			.required(false);
		
		ret.addArgument("--" + ARG_RANKS)
			.help("Include documents up to the specified rank in the deduplication.")
			.type(Integer.class)
			.setDefault(100)
			.required(false);
		
		ret.addArgument("--" + ARG_S3_THRESHOLD)
			.type(Double.class)
			.help("Report only near-duplicate pairs with s3 scores on word 8-grams above the specified threshold.")
			.setDefault(0.82);
		
		ret.addArgument("--" + ARG_THREADS)
			.type(Integer.class)
			.setDefault(1);

		ret.addArgument("--" + ARG_RUN_FILE)
			.type(Boolean.class)
			.setDefault(true)
			.help("Is the specified a run file (pass true), or a qrels file (pass false)");
		
		PreprocessingArgs.addArgs(ret);
		
		return ret;
	}

	static DocumentResolver docResolver(Namespace parsedArgs) {
		if("ChatNoirMapfiles".equals(parsedArgs.getString(ARG_DOC_RESOLVER))) {
			return new ChatNoirDocumentResolver();
		} else if ("AnseriniIndex".equals(parsedArgs.getString(ARG_DOC_RESOLVER))) {
			return new AnseriniDocumentResolver(parsedArgs.getString(ARG_ANSERINI_INDEX));
		}
		
		throw new RuntimeException("Unexpected " + ARG_DOC_RESOLVER + ": '" + parsedArgs.getString(ARG_DOC_RESOLVER) + "'.");
	}
}
