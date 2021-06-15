package de.webis.copycat.document_preprocessing;

import lombok.Data;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * 
 * @author Maik Fröbe
 */
@Data
public class PreprocessingArgs {
	
	public static final String
		ARG_KEEP_STOPWORDS = "keepStopwords",
		ARG_STOPWORDS = "stopwords",
		ARG_STEMMER = "stemmer",
		ARG_CONTENT_EXTRACTION = "contentExtraction";
	
	/**
	 * Switch: keep stopwords or remove them.
	 */
	public boolean keepStopwords = false;
	
	/**
	 * The list of stopwords is read from this file.
	 * When keepStopwords is false, and stopwords = null, then Anserinis default is used.
	 */
    public String stopwords = null;
    
    /**
     * The name of the stemmer passed to Anserini: "porter", "krovetz", or null.
     */
    public String stemmer = "porter";
    
    /**
     * The name of the content extraction: "Anserini", "Boilerpipe", "Jericho", "JustextVenv", or "No"
     */
    public String contentExtraction = "Anserini";

	public static PreprocessingArgs fromArgs(Namespace args) {
		PreprocessingArgs ret = new PreprocessingArgs();
		
		ret.setContentExtraction(args.getString(ARG_CONTENT_EXTRACTION));
		ret.setKeepStopwords(args.getBoolean(ARG_KEEP_STOPWORDS));
		ret.setStopwords(args.getString(ARG_STOPWORDS));
		ret.setStemmer(args.getString(ARG_STEMMER));
		if(ret.getStemmer().equalsIgnoreCase("null")) {
			ret.setStemmer(null);
		}
		
		return ret;
	}
	
	public static void addArgs(ArgumentParser parser) {
		parser
			.addArgument("--" + PreprocessingArgs.ARG_KEEP_STOPWORDS)
			.required(false)
			.setDefault(false)
			.help("Switch: keep stopwords or remove them.")
			.type(Boolean.class);
	
		parser
			.addArgument("--" + PreprocessingArgs.ARG_CONTENT_EXTRACTION)
			.required(false)
			.setDefault("Anserini")
			.choices("Anserini", "Boilerpipe", "Jericho", "Trafilatura", "TrafilaturaVenv", "JustextVenv", "No")
			.help("The name of the content extraction. (Use 'Anserini' for Anserini's default HTML to plain text transformation, or 'No' in case documents are already transformed (e.g., because they come from an anserini index)")
			.required(false);
		
		parser
			.addArgument("--" + PreprocessingArgs.ARG_STEMMER)
			.setDefault("porter")
			.choices("porter", "krovetz", "null")
			.help("The name of the stemmer (passed to Lucene with Anserini).")
			.required(false);
	
		parser
			.addArgument("--" + PreprocessingArgs.ARG_STOPWORDS)
			.help("The list of stopwords is read from this file. When keepStopwords is false, and stopwords = null, then Anserinis default is used.")
			.setDefault((String) null)
			.required(false);
	}
}
