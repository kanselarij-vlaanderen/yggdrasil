import { updateTriplestore } from '../triplestore';
import { decisionsReleaseFilter } from './release-validations';

/**
 * Helpers to collect data about:
 * - publication-flows
 * - identification
 *
 * Collection happens via the piece not the agendaitem!
 */

/*
 * Collect certain specific attributes of publication flows and their
 * identifiers related to the pieces from the distributor's source graph
 * in the temp graph.
 *
 * The piece collection must have already happened for this collection to
 * generate results. The filter to check if decisions have been released is
 * always applied.
 */
async function collectPublicationFlows(distributor) {
  const decisionsFilter = decisionsReleaseFilter(true);

  const properties = [
    [ '^pub:referentieDocument' ], // publication-flow
    [ '^pub:referentieDocument', 'adms:identifier' ] // identification
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX pub: <http://mu.semte.ch/vocabularies/ext/publicatie/>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      PREFIX adms: <http://www.w3.org/ns/adms#>
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX prov: <http://www.w3.org/ns/prov#>

      INSERT {
        GRAPH <${distributor.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${distributor.tempGraph}> {
          ?piece a dossier:Stuk ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${distributor.sourceGraph}> {
          ${decisionsFilter}
          ?piece ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

export {
  collectPublicationFlows,
}
