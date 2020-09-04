package beamlib.univocityio.coders;

import beamlib.univocityio.values.UnivocityCsvRow;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class UnivocityCsvRowCoder extends AtomicCoder<UnivocityCsvRow> {
    @Override
    public UnivocityCsvRow decode(InputStream inStream) throws CoderException, IOException {
        try (ObjectInputStream ois = new ObjectInputStream(inStream)) {
            UnivocityCsvRow row = (UnivocityCsvRow) ois.readObject();
            return row;
        } catch (ClassNotFoundException ex) {
            throw new CoderException(ex);
        }
    }

    @Override
    public void encode(UnivocityCsvRow value, OutputStream os) throws CoderException, IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
            oos.flush();
            os.write(baos.toByteArray());
        }
    }

    @Override
    public boolean consistentWithEquals() {
        return false;
    }
}
