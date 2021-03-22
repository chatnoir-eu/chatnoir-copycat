package de.webis.copycat.document_preprocessing;

import java.time.temporal.ChronoUnit;

import org.buildobjects.process.ProcBuilder;

import de.webis.copycat.DocumentPreprocessing;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

@SuppressWarnings("serial")
public abstract class DocumentPreprocessingWithDocker implements DocumentPreprocessing {

	public static final RetryPolicy<String> RETRY_POLICY = new RetryPolicy<String>()
			.handle(Exception.class)
			.withBackoff(3, 20, ChronoUnit.SECONDS)
			.withMaxRetries(2);
	
	@Override
	public String preprocessRawDocument(String text) {
		return preprocessRawDocument(text, RETRY_POLICY);
	}
	
	public String preprocessRawDocument(String text, RetryPolicy<String> policy) {
		return Failsafe.with(policy).get(() -> {
			ProcBuilder builder= new ProcBuilder("bash")
					.withArgs("-c", cmd())
	                .withTimeoutMillis(2*60*1000);
			builder.withInput(text);
			
			return builder.run().getOutputString();
		});
	}
	
	protected abstract String cmd();
}
