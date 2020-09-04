package beamlib.univocityio.values;

import java.io.Serializable;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.Compression;

public class UnivocityCsvSettings implements Serializable {
    private static final long serialVersionUID = 1L;

    private long divisor = 1L;
    private long remainder = 0L;
    private ResourceId sourceFile;
    private char delimiter = '\t';
    private String encoding = "UTF-8";
    private boolean headerRow = true;
    private boolean unsetQuote = false;
    private Compression compression;

    public UnivocityCsvSettings() {

    }

    public long getDivisor() {
        return divisor;
    }

    public void setDivisor(long divisor) {
        this.divisor = divisor;
    }

    public long getRemainder() {
        return remainder;
    }

    public void setRemainder(long remainder) {
        this.remainder = remainder;
    }

    public ResourceId getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(ResourceId sourceFile) {
        this.sourceFile = sourceFile;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(char delimiter) {
        this.delimiter = delimiter;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public boolean getHeaderRow() {
        return headerRow;
    }

    public void setHeaderRow(boolean headerRow) {
        this.headerRow = headerRow;
    }

    public boolean getUnsetQuote() {
        return unsetQuote;
    }

    public void setUnsetQuote(boolean unsetQuote) {
        this.unsetQuote = unsetQuote;
    }

    public Compression getCompression() {
        return compression;
    }

    public void setCompression(Compression compression) {
        this.compression = compression;
    }

    public void setCompressionAuto() {
        this.compression =
            (this.sourceFile.getFilename().endsWith(".gz")) ? Compression.GZIP : Compression.UNCOMPRESSED;
    }

    @Override
    public UnivocityCsvSettings clone() {
        UnivocityCsvSettings s = new UnivocityCsvSettings();
        s.divisor = divisor;
        s.remainder = remainder;
        s.sourceFile = sourceFile;
        s.delimiter = delimiter;
        s.encoding = encoding;
        s.headerRow = headerRow;
        s.unsetQuote = unsetQuote;
        s.compression = compression;

        return s;
    }
}
