package beamlib.univocityio.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface UnivocityIoOptions extends PipelineOptions {
	@Description("csv file name or wildcard")
	@Required
	String getInputFile();
	void setInputFile(String value);

	@Description("csv column delimiter")
	@Default.String("\t")
	String getDelimiter();
	void setDelimiter(String value);

	@Description("has csv header or not")
	@Default.Boolean(true)
	Boolean getWithHeader();
	void setWithHeader(Boolean value);

	@Description("number of tasks for one file")
	@Default.Long(1L)
	Long getTasksPerFile();
	void setTasksPerFile(Long value);
}
