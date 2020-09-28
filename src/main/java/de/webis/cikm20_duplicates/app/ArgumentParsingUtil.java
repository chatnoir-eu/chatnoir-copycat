package de.webis.cikm20_duplicates.app;

import lombok.experimental.UtilityClass;

import java.util.Collections;
import java.util.List;

import de.webis.cikm20_duplicates.util.warc.WARCParsingUtil.InputFormats;

@UtilityClass
public class ArgumentParsingUtil {
	public static final String TOOL_NAME = "CopyCat";
	public static final String ARG_INPUT = "input";
	public static final String ARG_FORMAT = "inputFormat";
	public static final String ARG_OUTPUT = "output";
	public static final String ARG_PARTITIONS = "partitions";
	public static final String ARG_MINIMUM_DOCUMENT_LENGTH = "minimumDocumentLength";
	public static final String ARG_NUM = "num";
	public static final String UUID_PREFIX = "uuidPrefix";
	public static final String UUID_INDEX = "uuidIndex";
	public static final List<String> ALL_INPUT_FORMATS = Collections.unmodifiableList(InputFormats.allInputFormats());
}
