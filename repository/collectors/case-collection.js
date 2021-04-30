import { updateTriplestore } from '../triplestore';

/**
 * Helpers to collect data about:
 * - cases
 * - subcases
 */

/*
 * Collect related cases and subcases for the relevant agendaitems
 * from the distributor's source graph in the temp graph.
 *
 * Note, all subcases and cases are copied. Restrictions regarding visibility
 * because of confidentiality are only taken into account
 * at the level of a file (nfo:FileDataObject)
 */
async function collectSubcasesAndCases(distributor) {
  const properties = [
    [ '^besluitvorming:genereertAgendapunt', 'besluitvorming:vindtPlaatsTijdens' ], // subcase
    [ '^besluitvorming:genereertAgendapunt', 'besluitvorming:vindtPlaatsTijdens', '^dossier:doorloopt' ] // case
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT {
        GRAPH <${distributor.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${distributor.tempGraph}> {
          ?agendaitem a besluit:Agendapunt ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${distributor.sourceGraph}> {
          ?agendaitem ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

export {
  collectSubcasesAndCases
}