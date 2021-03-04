package de.webis.copycat.document_preprocessing;

import de.webis.copycat.DocumentPreprocessing;

import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.Source;
import net.htmlparser.jericho.TextExtractor;

public class JerichoDocumentPreprocessing implements DocumentPreprocessing {

	@Override
	public String preprocessRawDocument(String html) {
        Source source = new Source(html);
        source.fullSequentialParse();

        TextExtractor extractor = source.getFirstElement(HTMLElementName.BODY).getTextExtractor();

        extractor.setConvertNonBreakingSpaces(true);
        extractor.setExcludeNonHTMLElements(false);
        extractor.setIncludeAttributes(false);

        return extractor.toString();
	}
}
