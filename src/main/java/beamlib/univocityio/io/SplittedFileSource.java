package beamlib.univocityio.io;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplittedFileSource extends BoundedSource<String> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SplittedFileSource.class);

    protected SplittedFileSource() {

    }

    @Override
    public List<? extends BoundedSource<String>> split(long desiredBundleSizeBytes, PipelineOptions options)
            throws Exception {

        LOG.info("splitting boundedsource");
        List<SplittedFileSource> splitResults = new ArrayList<>();
        splitResults.add(new SplittedFileSource());
        splitResults.add(new SplittedFileSource());
        splitResults.add(new SplittedFileSource());
        splitResults.add(new SplittedFileSource());
        splitResults.add(new SplittedFileSource());
        splitResults.add(new SplittedFileSource());
        splitResults.add(new SplittedFileSource());
        splitResults.add(new SplittedFileSource());
        return splitResults;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 0L;
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
