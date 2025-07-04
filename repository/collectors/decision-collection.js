import { queryTriplestore, updateTriplestore } from '../triplestore';
import { decisionsReleaseFilter } from './release-validations';
import { VIRTUOSO_RESOURCE_PAGE_SIZE } from '../../config';

/**
 * Helpers to collect data about:
 * - agendaitem-treatments
 * - decision-activities
 * - newsletters
 */

/*
 * Collect related agendaitem-treatments for the relevant agendaitems
 * from the distributor's source graph in the temp graph.
 *
 * If 'validateDecisionsRelease' is enabled on the distributor's release options
 * agendaitem-treatments are only copied if decisions of the meeting have already been released.
 *
 */
async function collectReleasedAgendaitemTreatments(distributor) {
  const properties = [
    [ '^dct:subject' ] // agendaitem-treatment
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX dct: <http://purl.org/dc/terms/>

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
          ${decisionsReleaseFilter(distributor.releaseOptions.validateDecisionsRelease)}
          ?agendaitem a besluit:Agendapunt .
          ?agendaitem ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

/*
 * Collect related decision-activities and newsitems for the relevant agendaitem-treatments
 * from the distributor's source graph in the temp graph.
 *
 * Whether the resources may be released already is validated during
 * the collection of agendaitem-treatments.
 */
async function collectAgendaitemDecisionActivitiesAndNewsitems(distributor) {
  const properties = [
    [ 'besluitvorming:heeftBeslissing' ], // decision-activity
    [ '^prov:wasDerivedFrom' ], // newsitem
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX prov: <http://www.w3.org/ns/prov#>

      INSERT {
        GRAPH <${distributor.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${distributor.tempGraph}> {
          ?treatment a besluit:BehandelingVanAgendapunt ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${distributor.sourceGraph}> {
          ?treatment a besluit:BehandelingVanAgendapunt .
          ?treatment ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

/**
 * Filter out a triple pointing to an empty treatment from the temp graph.
 * This is used to counter the cache issue when adding treatments to a graph
 * on a later (delta) run than the agendaitems.
 */
async function cleanupEmptyAgendaitemTreatments(distributor) {
  let offset = 0;

  const summary = await queryTriplestore(`
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX dct: <http://purl.org/dc/terms/>
    SELECT (COUNT(?s) AS ?count) WHERE {
      GRAPH <${distributor.tempGraph}> {
        ?s a besluit:Agendapunt .
        ?o dct:subject ?s .
        FILTER NOT EXISTS { ?o a besluit:BehandelingVanAgendapunt . }
      }
    }`);
  const count = summary.results.bindings.map(b => b['count'].value);

  while (offset < count) {
    await updateTriplestore(`
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX dct: <http://purl.org/dc/terms/>
    DELETE {
      GRAPH <${distributor.tempGraph}> {
        ?o dct:subject ?s .
      }
    }
    WHERE {
      GRAPH <${distributor.tempGraph}> {
        SELECT ?s ?o {
          ?s a besluit:Agendapunt .
          ?o dct:subject ?s .
          FILTER NOT EXISTS { ?o a besluit:BehandelingVanAgendapunt .}
        }
        LIMIT ${VIRTUOSO_RESOURCE_PAGE_SIZE}
      }
        }`);
    offset = offset + VIRTUOSO_RESOURCE_PAGE_SIZE;
  }
}

export {
  collectReleasedAgendaitemTreatments,
  collectAgendaitemDecisionActivitiesAndNewsitems,
  cleanupEmptyAgendaitemTreatments,
}
