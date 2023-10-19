import { updateTriplestore } from '../triplestore';
import { decisionsReleaseFilter } from './release-validations';

/**
 * Helpers to collect data about:
 * - publication-flows
 * - identification
 */

/*
 * Collect certain specific attributes of publication flows and their
 * identifiers related to the agendaitems from the distributor's source graph
 * in the temp graph.
 */
async function collectPublicationFlows(distributor) {
  const decisionsFilter = decisionsReleaseFilter(true);

  const pathToPubFlow = [ '^besluitvorming:genereertAgendapunt', 'besluitvorming:vindtPlaatsTijdens', '^dossier:doorloopt', '^dossier:Dossier.isNeerslagVan', '^dossier:behandelt' ];
  const properties = [
    pathToPubFlow, // publication-flow
    [ ...pathToPubFlow, 'adms:identifier' ] // identification
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX adms: <http://www.w3.org/ns/adms#>

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
          ${decisionsFilter}
          ?agendaitem ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

export {
  collectPublicationFlows,
}
