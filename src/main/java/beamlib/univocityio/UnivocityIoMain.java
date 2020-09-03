package beamlib.univocityio;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import beamlib.univocityio.io.SplittedFileIO;

import org.apache.beam.sdk.options.PipelineOptions;

public class UnivocityIoMain {

	private static final Logger LOG = LoggerFactory.getLogger(UnivocityIoMain.class);

	public static void main(String[] args) throws Exception {
		try {
			mainDataPipeline(args);
		} catch (Exception ex) {
			LOG.error("ERROR EXECUTING mainDataPipeline");
			LOG.error(ex.getMessage());
			throw ex;
		}
	}

	public static void mainDataPipeline(String[] args) throws Exception {
		LOG.info("start");

		UnivocityIoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(UnivocityIoOptions.class);
		Pipeline pipe = Pipeline.create(options);
		pipe.apply("split test", SplittedFileIO.read());
		pipe.run().waitUntilFinish();
	}

	public interface UnivocityIoOptions extends PipelineOptions {
	}
}
