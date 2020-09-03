package beamlib.univocityio.io;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplittedFileIO {
    private static final Logger LOG = LoggerFactory.getLogger(SplittedFileIO.class);

    public static Read read() {
        return new Read();
    }

    public static class Read extends PTransform<PBegin, PCollection<String>> {
        private static final long serialVersionUID = 1L;

        public Read() {

        }

        @Override
        public PCollection<String> expand(PBegin input) {
            LOG.info("expanding read");
            return input.apply("Read", org.apache.beam.sdk.io.Read.from(new SplittedFileSource()));
        }
    }

    private SplittedFileIO() {
    }
}
