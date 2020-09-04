package beamlib.univocityio.util;

import beamlib.univocityio.options.UnivocityIoOptions;
import beamlib.univocityio.values.UnivocityCsvRow;
import beamlib.univocityio.values.UnivocityCsvSettings;

public class ModelMapper {
    public static UnivocityCsvSettings map(UnivocityIoOptions options) {
        UnivocityCsvSettings settings = new UnivocityCsvSettings();

        if (options.getDelimiter() != null && options.getDelimiter().trim().length() > 0) {
            settings.setDelimiter(options.getDelimiter().trim().charAt(0));
        }

		// ヘッダの有り無し
		if (options.getWithHeader() != null) {
			settings.setHeaderRow(options.getWithHeader());
        }

        if (options.getTasksPerFile() != null) {
            settings.setDivisor(options.getTasksPerFile());
        }

        return settings;
    }
}
