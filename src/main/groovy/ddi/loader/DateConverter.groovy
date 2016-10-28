package ddi.loader

import java.text.SimpleDateFormat

/**
 * Convert a string date into date formatted like that: YYYYMMDD.
 *
 * Created by batman on 28/10/2016.
 */
class DateConverter {

    SimpleDateFormat sourceFormat = new SimpleDateFormat("dd/MM/yy")
    SimpleDateFormat destFormat = new SimpleDateFormat("yyyyMMdd")


    /**
     * Convert a date with this format DD/MM/YY into YYYYMMDD.
     *
     * @param s The date to convert as string.
     * @return The formatted date as number.
     */
    int convertDate(String s) {
        if (s) {
            def sourceDate = sourceFormat.parse(s)
            def destDate = destFormat.format(sourceDate)
            return Integer.parseInt(destDate)
        } else {
            return 99999999
        }
    }

}
