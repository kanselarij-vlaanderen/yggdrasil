import { VIRTUOSO_RESOURCE_PAGE_SIZE } from '../../config';
import { queryTriplestore, updateTriplestore } from '../triplestore';
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


/**
 * Filter out unwanted triples related to publication-flows from the temp graph.
 * We only want to propagate a subset of data about publcation-flows to other
 * graphs.
 */
async function cleanupPublicationFlowDetails(distributor) {
  const types = [
    'pub:Publicatieaangelegenheid',
    'pub:VertalingProcedurestap',
    'pub:PublicatieProcedurestap',
    'pub:VertaalActiviteit',
    'pub:DrukproefActiviteit',
    'pub:PublicatieActiviteit',
  ];
  const predicates = [
    'rdfs:comment',
    'dossier:openingsdatum',
    'dossier:sluitingsdatum',
    'fabio:hasPageCount',
    'pub:aantalUittreksels',
    'pub:publicatieWijze',
    'pub:urgentieniveau',
    'prov:hadActivity',
    'pub:threadId',
    'pub:doorlooptVertaling',
    'pub:doorlooptPublicatie',
    'dct:created',
    'dossier:Procedurestap.startdatum',
    'dossier:Procedurestap.einddatum',
    'tmo:targetEndTime',
    'tmo:dueDate',
    'pub:drukproefVerbeteraar',
    'pub:vertalingsactiviteitVanAanvraag',
    'pub:doelTaal',
    'pub:vertalingGebruikt',
    'pub:vertalingGenereert',
    'pub:drukproefGebruikt',
    'pub:drukproefGenereert',
    'pub:drukproefactiviteitVanAanvraag',
    'pub:publicatieGebruikt',
    'prov:generated',
    'pub:publicatieactiviteitVanAanvraag',
  ]

  const summary = await queryTriplestore(`
    PREFIX pub: <http://mu.semte.ch/vocabularies/ext/publicatie/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX fabio: <http://purl.org/spar/fabio/>
    PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX tmo: <http://www.semanticdesktop.org/ontologies/2008/05/20/tmo#>

    SELECT (COUNT(?o) AS ?count) WHERE {
      GRAPH <${distributor.tempGraph}> {
        ?s a ?type .
        VALUES ?type {
          ${types.join('\n')}
        }
        ${predicates.map((pred) => ` { ?s ${pred} ?o } `).join('\n UNION ')}
      }
    }`);
  const count = summary.results.bindings.map(b => b['count'].value);

  let offset = 0;
  while (offset < count) {
    await updateTriplestore(`
    PREFIX pub: <http://mu.semte.ch/vocabularies/ext/publicatie/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX fabio: <http://purl.org/spar/fabio/>
    PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX tmo: <http://www.semanticdesktop.org/ontologies/2008/05/20/tmo#>
    DELETE {
      GRAPH <${distributor.tempGraph}> {
        ?s ?p ?o .
      }
    }
    WHERE {
      GRAPH <${distributor.tempGraph}> {
        SELECT ?s ?p ?o {
          ?s a ?type .
          VALUES ?type {
            ${types.join('\n')}
          }
          VALUES ?p {
            ${predicates.join('\n')}
          }
          OPTIONAL { ?s ?p ?o }
        }
        LIMIT ${VIRTUOSO_RESOURCE_PAGE_SIZE}
      }
    }`);
    offset = offset + VIRTUOSO_RESOURCE_PAGE_SIZE;
  }
}

export {
  collectPublicationFlows,
  collectPublicationFlowSubcasesAndActivities,
  cleanupPublicationFlowDetails,
}
