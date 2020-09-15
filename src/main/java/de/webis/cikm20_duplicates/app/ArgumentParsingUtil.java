package de.webis.cikm20_duplicates.app;

import de.webis.chatnoir2.mapfile_generator.inputformats.WarcInputFormat;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.UtilityClass;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import de.webis.chatnoir2.mapfile_generator.inputformats.ClueWeb09InputFormat;
import de.webis.chatnoir2.mapfile_generator.inputformats.ClueWeb12InputFormat;
import de.webis.chatnoir2.mapfile_generator.inputformats.CommonCrawlInputFormat;

@UtilityClass
class ArgumentParsingUtil {
	public static final String TOOL_NAME = "CopyCat";
	public static final String ARG_INPUT = "input";
	public static final String ARG_FORMAT = "inputFormat";
	public static final String ARG_OUTPUT = "output";
	public static final String ARG_PARTITIONS = "partitions";
	
	
	@Getter
	@AllArgsConstructor
	static enum InputFormats {
		CLUEWEB09(ClueWeb09InputFormat.class),
		CLUEWEB12(ClueWeb12InputFormat.class),
		COMMON_CRAWL(CommonCrawlInputFormat.class);
		
		private final Class<? extends WarcInputFormat> inputFormat;
		
		public static List<String> allInputFormats() {
			return Arrays.asList(InputFormats.values()).stream()
					.map(i -> i.name())
					.collect(Collectors.toList());
		}
	}
}
