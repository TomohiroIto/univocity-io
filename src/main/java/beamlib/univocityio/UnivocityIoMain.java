package beamlib.univocityio;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.common.io.CharStreams;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

		UnivocityIoOptions options = PipelineOptionsFactory
				.fromArgs(args)
				.withValidation()
				.as(UnivocityIoOptions.class);

        Pipeline pipe = Pipeline.create(options);

		// final String tableSchemaJson = readJsonSchema(options.getJsonSchemaPath());
		// final List<TableFieldSchemaCustom> tableSchema = new ObjectMapper()
		// 		.reader()
		// 		.forType(new TypeReference<List<TableFieldSchemaCustom>>() {})
		// 		.readValue(tableSchemaJson);

		// if (timePartitioned) {
		// 	// タイムパーティションを利用する場合のパイプライン
		// 	pipe.apply("Read CSV", CsvIO.read().withOptions(options).from(options.getInputFile()))
		// 		.apply("Data Conversion", new CsvRowTransform(tableSchema))
		// 		.apply("Write to BigQuery", BigQueryIO.writeTableRows().to(
		// 			(SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination>) input -> new TableDestination(
		// 					fullTableName,
		// 					null,
		// 					new TimePartitioning().setType("DAY")))
		// 			.withSchema(TableFieldSchemaCustom.toTableSchema(tableSchema))
		// 			.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
		// 			.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		// } else {
		// 	// 日付付きテーブルを利用する場合のパイプライン
		// 	pipe.apply("Read CSV", CsvIO.read().withOptions(options).from(options.getInputFile()))
		// 		.apply("Data Conversion", new CsvRowTransform(tableSchema))
		// 		.apply("Write to BigQuery", BigQueryIO.writeTableRows().to(
		// 				fullTableName)
		// 				.withSchema(TableFieldSchemaCustom.toTableSchema(tableSchema))
		// 				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
		// 				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		// }

		pipe.run().waitUntilFinish();
	}

    public interface UnivocityIoOptions extends PipelineOptions {
    }
}
