package de.webis.copycat.document_preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import de.webis.copycat.DocumentPreprocessing;
import io.anserini.analysis.EnglishStemmingAnalyzer;
import lombok.SneakyThrows;

/**
 * This DocumentPreprocessing wraps an arbitrary internal DocumentPreprocessing
 * and applies stemming (porter, krovetz, or "" for none) and stopword removal on
 * the text provided by the internal DocumentPreprocessing.
 * 
 * The internal DocumentPreprocessing usually transforms HTML pages to text.
 * 
 * @author Maik Fröbe
 *
 */
@SuppressWarnings("serial")
class StemmingAndStopWordRemovalDocumentPreprocessing  implements DocumentPreprocessing {

	private final DocumentPreprocessing internalPreprocessing;
	
	private final String stemmer;
	
	private final ArrayList<String> stopWords;
	
	StemmingAndStopWordRemovalDocumentPreprocessing(DocumentPreprocessing internalPreprocessing) {
		this(internalPreprocessing, "");
	}
	
	StemmingAndStopWordRemovalDocumentPreprocessing(DocumentPreprocessing internalPreprocessing, String stemmer) {
		this.internalPreprocessing = internalPreprocessing;
		this.stemmer = stemmer;
		this.stopWords = new ArrayList<>(Collections.emptyList());
	}
	
	StemmingAndStopWordRemovalDocumentPreprocessing(DocumentPreprocessing internalPreprocessing, List<String> stopWords, String stemmer) {
		this.internalPreprocessing = internalPreprocessing;
		this.stemmer = stemmer;
		this.stopWords = new ArrayList<>(stopWords);
	}
	
	@SneakyThrows
	private String stemmAndRemoveStopWords(String text) {
		CharArraySet noStopWords = new CharArraySet(stopWords, false);
		
		io.anserini.analysis.EnglishStemmingAnalyzer analyzer = new EnglishStemmingAnalyzer(stemmer, noStopWords);
		List<String> tokens = tokensInText(analyzer, text);
		
		return tokens.stream().collect(Collectors.joining(" "));
	}
	
	private static List<String> tokensInText(Analyzer analyzer, String text) throws IOException {
		List<String> ret = new LinkedList<>();
		TokenStream tokenStream = analyzer.tokenStream("", text);

		CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
		tokenStream.reset();

		while (tokenStream.incrementToken()) {
			ret.add(attr.toString());
		}

		return ret;
	}

	@Override
	public String preprocessRawDocument(String text) {
		text = internalPreprocessing.preprocessRawDocument(text);
		return stemmAndRemoveStopWords(text);
	}
}
