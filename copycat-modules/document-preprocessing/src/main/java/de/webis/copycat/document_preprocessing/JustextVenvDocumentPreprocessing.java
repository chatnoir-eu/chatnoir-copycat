package de.webis.copycat.document_preprocessing;

/**
 * hhttps://pypi.org/project/jusText/
 */
@SuppressWarnings("serial")
public class JustextVenvDocumentPreprocessing extends DocumentPreprocessingWithDocker {
	@Override
	protected String cmd() {
		return "/mnt/ceph/storage/data-in-progress/data-teaching/theses/wstud-thesis-wagner/justext-venv/execute-justext-in-venv.sh";
	}
}
