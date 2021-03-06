package ddi.loader

/**
 * Created by batman on 23/10/2016.
 */
class CsvHeader {

    private final String[] header

    CsvHeader(String headerLine) {
        header = headerLine.split(/;/)
    }

    Map<String, String> values(String[] fields) {
        def values = new HashMap<String, String>()

        for (int i = 0; i < fields.length; ++i) {
            def field = header[i]
            def value = fields[i]

            values[field] = value
        }

        return values
    }
}
