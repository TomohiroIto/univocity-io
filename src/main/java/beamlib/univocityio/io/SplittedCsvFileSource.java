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
import java.util.Collections;
import org.apache.beam.sdk.coders.SerializableCoder;
import beamlib.univocityio.values.UnivocityCsvRow;
import beamlib.univocityio.values.UnivocityCsvSettings;
import beamlib.univocityio.extensions.UnivocityCsvParser;
import beamlib.univocityio.coders.UnivocityCsvRowCoder;

public class SplittedCsvFileSource extends BoundedSource<UnivocityCsvRow> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SplittedCsvFileSource.class);

    private UnivocityCsvSettings settings;

    SplittedCsvFileSource(UnivocityCsvSettings settings) {
        this.settings = settings;
    }

    @Override
    public List<? extends BoundedSource<UnivocityCsvRow>> split(
            long desiredBundleSizeBytes,
            PipelineOptions options)
            throws Exception {

        LOG.warn("splitting boundedsource SplittedCsvFileSource");

        return Collections.singletonList(this);
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
        LOG.info("create Reader SplittedCsvFileSource");
        return new SplittedCsvFileReader(this, settings);
    }

    public static class SplittedCsvFileReader extends BoundedReader<UnivocityCsvRow> {
        private SplittedCsvFileSource source = null;
        private UnivocityCsvSettings settings;
        private transient UnivocityCsvParser parser = null;
        private String fileName;

        public SplittedCsvFileReader(SplittedCsvFileSource source, UnivocityCsvSettings settings) {
            this.source = source;
            this.settings = settings;
            this.fileName = settings.getSourceFile().getFilename();
        }

        @Override
        public BoundedSource<UnivocityCsvRow> getCurrentSource() {
            return source;
        }

        @Override
        public boolean start() throws IOException {
            LOG.info("start");
            close();

            parser = new UnivocityCsvParser(
                settings.getSourceFile(),
                settings.getCompression(),
                settings.getDivisor(),
                settings.getRemainder(),
                settings.getDelimiter(),
                settings.getEncoding(),
                settings.getHeaderRow(),
                settings.getUnsetQuote()
            );

            return true;
        }

        @Override
        public boolean advance() throws IOException {
            return parser.advance();
        }

        @Override
        public UnivocityCsvRow getCurrent() throws NoSuchElementException {
            String[] row = parser.getCurrent();
            if (row == null) {
                throw new NoSuchElementException();
            }

            return new UnivocityCsvRow(row, fileName, parser.getCurrentRowIndex());
        }

        @Override
        public void close() throws IOException {
            if (parser != null) parser.close();
            parser = null;
        }
    }
}
