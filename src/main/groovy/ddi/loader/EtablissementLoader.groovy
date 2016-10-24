package ddi.loader

/**
 * Created by batman on 23/10/2016.
 */
class EtablissementLoader implements Loader {

    private final String[] header

    EtablissementLoader() {
        def names = 'SIRET|BASE_ORIGINE|RAISON_SOCIALE|RAISON_SOCIALE2|LIBELLE_BATIMENT|LIBELLE_ADRESSE|LIBELLE_SPECIFIQUE|' +
                'CODE_POSTAL|CENTRE_DISTRIBUTEUR|CODE_POSTAL_THEORIQUE|COLLECTIVITE|TYPOLOGIE|CODE_REGION|CODE_DEPARTEMENT|' +
                'CODE_COMMUNE|TRANCHE_TAILLE_COMMUNE|CONTACT_EXISTANT|STATUT_ETABLISSEMENT|CODE_DOMAINE_ACTIVITE|NAF_ENTREPRISE|' +
                'TRANCHE_EFF_ENTREPRISE|NAF_ETABLISSEMENT|TRANCHE_EFF_ETABLISSEMENT|NAF_MIXTE|TRANCHE_EFF_MIXTE|CODE_CAT_JURIDIQUE|' +
                'TELEPHONE|FAX|PRINCIPAL_DIRIGEANT|PERSONNE_MORALE|TRANCHE_CA|TRANCHE_PCT_CA_EXPORT|DATE_CREA_ETABLISSEMENT|' +
                'DATE_CREA_ENTREPRISE|CODE_ORIG_CREA_ENTREPRISE|NES_ENTREPRISE|NES_ETABLISSEMENT|NES_MIXTE|CANTON|ARRONDISSEMENT|' +
                'UNITE_URBAINE|CODE_TAILLE_UNITE_URBAINE|CODE_ZONE_EMPLOI|TYPE_ETAB_PUBLIC|TYPE_ASSOCIATION|TYPE_ENTREPRISE_1|' +
                'TYPE_ENTREPRISE_2|MONDE_AGRICOLE|CREATION_ENTREPRISE|EMAIL|SITEWEB|IRIS|CA_ORT'

        header = names.split(/\|/, -1)
    }

    @Override
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
