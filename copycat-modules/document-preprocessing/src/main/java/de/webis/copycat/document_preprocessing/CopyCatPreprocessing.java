package de.webis.copycat.document_preprocessing;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.en.EnglishAnalyzer;

import com.google.common.collect.ImmutableList;

import de.webis.copycat.DocumentPreprocessing;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.inf.Namespace;

public class CopyCatPreprocessing {
	public static DocumentPreprocessing documentPreprocessing(Namespace args) {
		return documentPreprocessing(PreprocessingArgs.fromArgs(args));
	}
	
	public static DocumentPreprocessing documentPreprocessing(PreprocessingArgs args) {
		DocumentPreprocessing contentExtraction = contentExtraction(args.contentExtraction);
		if(args.getContentExtraction().equalsIgnoreCase("no")) {
			return contentExtraction;
		}
		
		String stemmer = args.getStemmer() == null ? "" : args.getStemmer().trim();
		List<String> stopwords = stopwords(args);
		
		return new StemmingAndStopWordRemovalDocumentPreprocessing(contentExtraction, stopwords, stemmer);
	}
	
	private static List<String> stopwords(PreprocessingArgs args) {
		if(!args.keepStopwords && args.stopwords == null) {
			return defaultEnglishStopwords();
		} else if (!args.keepStopwords && args.stopwords != null) {
			return stopwordsFromFile(args.getStopwords());
		} else {
			return Collections.emptyList();
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static List<String> defaultEnglishStopwords() {
		return (List) ImmutableList.copyOf(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET.iterator());
	}
	
	@SneakyThrows
	private static List<String> stopwordsFromFile(String file) {
		return Files.readAllLines(Paths.get(file)).stream()
				.map(i -> i.trim())
				.collect(Collectors.toList());
	}
	
	@SneakyThrows
	private static DocumentPreprocessing contentExtraction(String clazz) {
		return (DocumentPreprocessing) Class.forName("de.webis.copycat.document_preprocessing." + clazz + "DocumentPreprocessing").newInstance();
	}
//	
//	lese hier aus argumenten/Namespace die komplette konfiguration aus, die dann auch verwendet über args-array in unit-tests.
//	füge in notebooks tests ein, die das preprocessing austesten, und dann auf die doku verweisen
}
