package ddi.loader;

import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

/**
 * Created by batman on 28/10/2016.
 */
public class DateConverterTest {
    @Test
    public void convertDate() throws Exception {
        DateConverter converter = new DateConverter();

        assertEquals(20161028, converter.convertDate("28/10/16"));
    }

}