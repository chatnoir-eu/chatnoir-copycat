package de.webis.copycat.document_preprocessing;

/**
 * https://github.com/adbar/trafilatura is executed with docker
 */
@SuppressWarnings("serial")
public class TrafilaturaDocumentPreprocessing extends DocumentPreprocessingWithDocker {
	@Override
	protected String cmd() {
		return "docker run --rm -i 777pythonrw/python-mce:latest";
	}
}
