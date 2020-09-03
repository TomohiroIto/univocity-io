package beamlib.univocityio.extensions;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnivocityCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(UnivocityCsvParser.class);

    // current_index = quotient * divisor + remainder
    private long divisor = 1L;
    private long remainder = 0L;
    private long quotient = -1L;
    private String[] current = null;

    private ResourceId sourceFile;
    private String fileName;
    private Compression compression;
    private CsvParser parser = null;
    private char delimiter = '\t';
    private String encoding = "UTF-8";
    private boolean headerRow = true;
    private boolean unsetQuote = false;

    private ReadableByteChannel readChannel = null;
    private InputStream readStream = null;
    private InputStreamReader readReader = null;

    public UnivocityCsvParser(
            ResourceId sourceFile,
            Compression compression,
            long divisor,
            long remainder,
            char delimiter,
            String encoding,
            boolean headerRow,
            boolean unsetQuote) {

        this.sourceFile = sourceFile;
        this.fileName = sourceFile.getFilename();
        this.compression = compression;
        this.divisor = divisor;
        this.remainder = remainder;
        this.quotient = -1L;
        this.delimiter = delimiter;
        this.encoding = encoding;
        this.headerRow = headerRow;
        this.unsetQuote = unsetQuote;
    }

    public UnivocityCsvParser(
            ResourceId sourceFile,
            Compression compression,
            char delimiter,
            String encoding,
            boolean headerRow,
            boolean unsetQuote) {

        this.sourceFile = sourceFile;
        this.fileName = sourceFile.getFilename();
        this.compression = compression;
        this.divisor = 1L;
        this.remainder = 0L;
        this.quotient = -1L;
        this.delimiter = delimiter;
        this.encoding = encoding;
        this.headerRow = headerRow;
        this.unsetQuote = unsetQuote;
    }

    public boolean start() throws IOException {
        if (parser != null)
            close();

        // read file from stream
        readChannel = compression.readDecompressed(FileSystems.open(sourceFile));
        readStream = Channels.newInputStream(readChannel);
        readReader = new InputStreamReader(readStream, Charset.forName(encoding));

        // Univocity Parser
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        settings.getFormat().setDelimiter(delimiter);

        if (unsetQuote == true) {
            settings.getFormat().setQuote('\u0000');
            settings.getFormat().setQuoteEscape('\u0000');
            settings.getFormat().setCharToEscapeQuoteEscaping('\u0000');
        } else {
            settings.getFormat().setQuote('"');
            settings.getFormat().setQuoteEscape('"');
            settings.getFormat().setCharToEscapeQuoteEscaping('"');
        }

        settings.setHeaderExtractionEnabled(headerRow);

        // settings limitation (fixed values)
        settings.setMaxColumns(1000);
        settings.setMaxCharsPerColumn(100000000);
        parser = new CsvParser(settings);
        parser.beginParsing(readReader);
        quotient = -1L;

        return true;
    }

    public void close() throws IOException {
        try {
            if (parser != null)
                parser.stopParsing();
            if (readReader != null)
                readReader.close();
            if (readStream != null)
                readStream.close();
            if (readChannel != null)
                readChannel.close();
            parser = null;
        } catch (Exception ex) {
            // ignore
        }
    }

    public boolean advance() throws IOException {
        long limit = (quotient == -1L) ? remainder + 1 : divisor;
        for (long i = 0; i < limit; i++) {
            if (readNext() == false)
                break;
        }
        quotient++;

        return current != null;
    }

    private boolean readNext() throws IOException {
        try {
            current = parser.parseNext();
            return current != null;
        } catch (Exception ex) {
            LOG.error("ERROR READING CSV: file={}, offset={}", this.fileName, this.quotient);
            LOG.error(ex.getMessage());
            throw new IOException(ex);
        }
    }

    public String[] getCurrent() throws NoSuchElementException {
        if (current == null) {
            LOG.warn("getCurrent RETURNS NULL");
        }

        return current;
    }
}
