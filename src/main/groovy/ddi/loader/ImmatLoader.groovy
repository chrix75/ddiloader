package ddi.loader

import sun.java2d.pipe.SpanShapeRenderer.Simple

import java.text.SimpleDateFormat

/**
 * Created by batman on 23/10/2016.
 */
class ImmatLoader implements Loader {
    private final String[] header
    private SimpleDateFormat fabDateFormat = new SimpleDateFormat('dd/MM/yy')
    private SimpleDateFormat dbDateFormat = new SimpleDateFormat('yyyyMMdd')

    ImmatLoader() {
        def names = 'IM_NUMIMMAT|IM_CLEPROD|IM_TYPVIN|IM_NOM|IM_SOCTIT|IM_SIRENTIT|IM_SIRENCCD|IM_DATCARTGRIS|IM_ANCARTGRIS|' +
                'IM_MOISCARTGRIS|IM_DAT1MCIR|IM_AN1MCIR|IM_MOIS1MCIR|IM_PTR|IM_NEUFOCCAS|IM_CDMARQUE|IM_CDMODELE|IM_CDGAMME|' +
                'IM_CDTYPTRAV|IM_CDGENRE|IM_CDTOPGENRE|IM_CDTYPLOC|IM_CDCATEGCLI|IM_CDTYPENE|IM_CDTYPCAR|IM_CDTYPCARCG|' +
                'IM_OPPOSITION|IM_DIFFMKT|IM_DIFFDDI|IM_TYPEXCLU|IM_TXCO2'

        header = names.split(/\|/, -1)

    }

    @Override
    Map<String, String> values(String[] fields) {
        def values = [:]

        for (int i = 0; i < header.length; ++i) {
            def field = header[i]

            if (isFabDate(field)) {
                def date = fabDateFormat.parse(fields[i])
                values.put(field, dbDateFormat.format(date))
            } else {
                values.put(field, fields[i])
            }
        }

        return values
    }

    private boolean isFabDate(String s) {
        s == 'IM_DATCARTGRIS' || s == 'IM_DAT1MCIR'
    }
}
