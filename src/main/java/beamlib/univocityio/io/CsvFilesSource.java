package beamlib.univocityio.io;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import beamlib.univocityio.options.UnivocityIoOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;

import org.apache.beam.sdk.io.FileSystems;

import org.apache.beam.sdk.coders.SerializableCoder;
import beamlib.univocityio.values.UnivocityCsvRow;
import beamlib.univocityio.values.UnivocityCsvSettings;
import beamlib.univocityio.coders.UnivocityCsvRowCoder;

public class CsvFilesSource extends BoundedSource<UnivocityCsvRow> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CsvFilesSource.class);

    private UnivocityCsvSettings settings;
    private ValueProvider<String> fileOrPatternSpec;

    protected CsvFilesSource(
            ValueProvider<String> fileSpec,
            UnivocityCsvSettings settings) {

        this.fileOrPatternSpec = fileSpec;
        this.settings = settings;
    }

    @Override
    public List<? extends BoundedSource<UnivocityCsvRow>> split(
            long desiredBundleSizeBytes,
            PipelineOptions options)
            throws Exception {

        LOG.info("splitting boundedsource CsvFilesSource");
        List<SplittedCsvFileSource> splitResults = new ArrayList<>();

        String fileOrPattern = fileOrPatternSpec.get();
        List<Metadata> expandedFiles = FileSystems
            .match(
                fileOrPattern,
                EmptyMatchTreatment.ALLOW)
            .metadata();
        for (Metadata metadata : expandedFiles) {
            // split files
            for (long remainder = 0; remainder < settings.getDivisor(); remainder++) {
                UnivocityCsvSettings settingsWorker = settings.clone();
                settingsWorker.setSourceFile(metadata.resourceId());
                settingsWorker.setRemainder(remainder);
                settingsWorker.setCompressionAuto();

                splitResults.add(new SplittedCsvFileSource(settingsWorker));
            }
        }

        return splitResults;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 0L;
    }

    @Override
    public Coder<UnivocityCsvRow> getOutputCoder() {
        return new UnivocityCsvRowCoder();
    }

    @Override
    public BoundedReader<UnivocityCsvRow> createReader(PipelineOptions options) throws IOException {
        LOG.warn("create Reader CsvFilesSource");
        return null;
    }
}
