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
  const properties = [
    [ '^pub:referentieDocument' ], // publication-flow
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX pub: <http://mu.semte.ch/vocabularies/ext/publicatie/>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>

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
          ${decisionsReleaseFilter(distributor.releaseOptions.validateDecisionsRelease)}
          ?piece a dossier:Stuk .
          ?piece ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

async function collectPublicationFlowSubcasesAndActivities(distributor) {
  const properties = [
    [ 'pub:doorlooptVertaling' ], // translation-subcase
    [ 'pub:doorlooptVertaling', '^pub:vertalingVindtPlaatsTijdens' ], // translation-activities
    [ 'pub:doorlooptPublicatie' ], // publication-subcase
    [ 'pub:doorlooptPublicatie', '^pub:drukproefVindtPlaatsTijdens' ], // proofing-activities
    [ 'pub:doorlooptPublicatie', '^pub:publicatieVindtPlaatsTijdens' ], // publication-activities
    [ 'adms:identifier' ], // identification
    [ 'adms:identifier', 'generiek:gestructureerdeIdentificator' ], // structured-identifier
    [ 'pub:identifier' ], // numac-numbers
    [ 'prov:qualifiedDelegation' ], // contact-persons
    [ 'prov:qualifiedDelegation', '^schema:contactPoint' ], // contact-persons users
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX pub: <http://mu.semte.ch/vocabularies/ext/publicatie/>
      PREFIX adms: <http://www.w3.org/ns/adms#>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX generiek:  <https://data.vlaanderen.be/ns/generiek#>
      PREFIX schema: <http://schema.org/>

      INSERT {
        GRAPH <${distributor.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${distributor.tempGraph}> {
          ?pubFlow a pub:Publicatieaangelegenheid ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${distributor.sourceGraph}> {
          ?pubFlow a pub:Publicatieaangelegenheid .
          ?pubFlow ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

export {
  collectPublicationFlows,
  collectPublicationFlowSubcasesAndActivities
}
