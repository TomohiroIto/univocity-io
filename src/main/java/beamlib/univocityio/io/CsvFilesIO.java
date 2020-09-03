package beamlib.univocityio.io;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import beamlib.univocityio.values.UnivocityCsvRow;
import beamlib.univocityio.options.UnivocityIoOptions;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import beamlib.univocityio.values.UnivocityCsvSettings;

public class CsvFilesIO {
    private static final Logger LOG = LoggerFactory.getLogger(CsvFilesIO.class);

    public static Read read(UnivocityIoOptions options) {
        return new Read(options);
    }

    public static class Read extends PTransform<PBegin, PCollection<UnivocityCsvRow>> {
        private static final long serialVersionUID = 1L;

        UnivocityIoOptions options;
        public Read(UnivocityIoOptions options) {
            this.options = options;
        }

        @Override
        public PCollection<UnivocityCsvRow> expand(PBegin input) {
            LOG.info("expanding read");
            // TODO: calc settings
            return input
                .apply("Read", org.apache.beam.sdk.io.Read.from(new CsvFilesSource(
                    StaticValueProvider.of(options.getInputFile()),
                    new UnivocityCsvSettings()
                )));
        }
    }

    private CsvFilesIO() {
    }
}
