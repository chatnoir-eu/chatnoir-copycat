package de.webis.copycat.document_preprocessing;

/**
 * https://github.com/adbar/trafilatura is executed with docker
 */
@SuppressWarnings("serial")
public class TrafilaturaVenvDocumentPreprocessing extends DocumentPreprocessingWithDocker {
	@Override
	protected String cmd() {
		return "/mnt/ceph/storage/data-in-progress/data-teaching/theses/wstud-thesis-wagner/trafilatura-venv/execute-trafilatura-in-venv.sh";
	}
}
