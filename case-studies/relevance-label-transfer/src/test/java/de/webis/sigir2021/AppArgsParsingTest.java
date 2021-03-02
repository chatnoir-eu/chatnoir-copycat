package de.webis.sigir2021;

import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.sigir2021.App;
import de.webis.sigir2021.trec.JudgedDocuments;
import net.sourceforge.argparse4j.inf.Namespace;

public class AppArgsParsingTest {
	@Test
	public void testWeb2009WithTopic34() {
		List<String> judgedDocuments = judgedDocuments(
			"-o", "foo-bar",
			"--task", "WEB_2009",
			"--topic", "34"
		);
		
		Approvals.verifyAsJson(judgedDocuments);
	}

	@Test
	public void testWeb2010WithTopic64() {
		List<String> judgedDocuments = judgedDocuments(
			"-o", "foo-bar",
			"--task", "WEB_2010",
			"--topic", "64"
		);
		
		Approvals.verifyAsJson(judgedDocuments);
	}

	
	private static List<String> judgedDocuments(String...args) {
		Namespace parsedArgs = App.validArgumentsOrNull(args);
		
		return JudgedDocuments.judgedDocuments(parsedArgs);
	}
}
