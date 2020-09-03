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

public class CsvFilesSource extends BoundedSource<String> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SplittedFileSource.class);

    // private ResourceId sourceFile;

	// private char delimiter = '\t';

	// private String encoding = "UTF-8";

	// private boolean headerRow = true;

    // private boolean unsetQuote = false;
    
    // private long tasksPerFile = 1L;

    // private String inputFiles = null;


    private UnivocityIoOptions options;

    private final ValueProvider<String> fileOrPatternSpec;

    protected CsvFilesSource(
            ValueProvider<String> fileSpec,
            UnivocityIoOptions options) {

        this.fileOrPatternSpec = fileSpec;
        this.options = options;

        // if (options != null) {
        //     // デリミタ
        //     if (options.getDelimiter() != null &&
        //         options.getDelimiter().trim().length() > 0) {

        //         this.delimiter = options.getDelimiter().trim().charAt(0);
        //     }

        //     // ヘッダの有り無し
        //     if (options.getWithHeader() != null) {
        //         this.headerRow = options.getWithHeader();
        //     }

        //     // ファイル分割数
        //     if (options.getTasksPerFile() != null) {
        //         this.tasksPerFile = options.getTasksPerFile();
        //     }

        //     // File name
        //     if (options.getInputFile() != null) {
        //         this.inputFiles = options.getInputFile();
        //     }
        // }
    }

    @Override
    public List<? extends BoundedSource<String>> split(
            long desiredBundleSizeBytes,
            PipelineOptions options)
            throws Exception {

        LOG.info("splitting boundedsource");
        List<SplittedFileSource> splitResults = new ArrayList<>();

        String fileOrPattern = fileOrPatternSpec.get();
        List<Metadata> expandedFiles = FileSystems
            .match(
                fileOrPattern,
                EmptyMatchTreatment.ALLOW)
            .metadata();
        for (Metadata metadata : expandedFiles) {
        }

        // splitResults.add(new SplittedFileSource());
        // splitResults.add(new SplittedFileSource());
        // splitResults.add(new SplittedFileSource());
        // splitResults.add(new SplittedFileSource());
        // splitResults.add(new SplittedFileSource());
        // splitResults.add(new SplittedFileSource());
        // splitResults.add(new SplittedFileSource());
        // splitResults.add(new SplittedFileSource());
        return splitResults;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 0L;
    }

    @Override
    public Coder<String> getOutputCoder() {
        return StringUtf8Coder.of();
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
        LOG.info("create Reader");
        return new SplittedFileReader(this);
    }

    public static class SplittedFileReader extends BoundedReader<String> {
        SplittedFileSource source = null;

        public SplittedFileReader(SplittedFileSource source) {
            this.source = source;
        }

        @Override
        public BoundedSource<String> getCurrentSource() {
            return source;
        }

        private int currentpos = 0;

        @Override
        public boolean start() throws IOException {
            LOG.info("start");
            currentpos = 0;
            return true;
        }

        @Override
        public boolean advance() throws IOException {
            LOG.info(String.format("pos = %d", currentpos));
            return currentpos++ < 9;
        }

        @Override
        public String getCurrent() throws NoSuchElementException {
            if (currentpos <= 10)
                return "test";

            throw new NoSuchElementException();
        }

        @Override
        public void close() throws IOException {

        }
    }
}
