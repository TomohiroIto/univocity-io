package beamlib.univocityio.values;

import java.io.Serializable;

public class UnivocityCsvRow implements Serializable {
    private static final long serialVersionUID = 1L;
    private String[] values;
    private String fileName;
    private long rowNumber;

    public UnivocityCsvRow(String[] values, String fileName, long rowNumber) {
        this.values = values;
        this.fileName = fileName;
        this.rowNumber = rowNumber;
    }

    public String[] getValues() {
        return values;
    }

    public String getFileName() {
        return fileName;
    }

    public long getRowNumber() {
        return rowNumber;
    }

    public void setValues(String[] values) {
        this.values = values;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setRowNumber(long rowNumber) {
        this.rowNumber = rowNumber;
    }
}
