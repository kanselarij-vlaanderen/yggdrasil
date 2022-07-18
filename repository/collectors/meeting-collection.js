import { updateTriplestore } from '../triplestore';
import { decisionsReleaseFilter } from './release-validations';

/**
 * Helpers to collect data about:
 * - meetings
 * - newsletters
 */

/*
 * Collect related meetings for the relevant agenda's
 * from the distributor's source graph in the temp graph.
 */
async function collectMeetings(distributor) {
  const properties = [
    [ 'besluitvorming:isAgendaVoor' ], // meeting
    [ '^besluitvorming:behandelt' ], // meeting
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

      INSERT {
        GRAPH <${distributor.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${distributor.tempGraph}> {
          ?agenda a besluitvorming:Agenda ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${distributor.sourceGraph}> {
          ?agenda ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

/*
 * Collect related newsletters for the relevant meetings
 * from the distributor's source graph in the temp graph.
 *
 * If 'validateDecisionsRelease' is enabled on the distributor's release options
 * newsitems are only copied if the decisions of the meeting have already been released.
 */
async function collectReleasedNewsletter(distributor) {
  const properties = [
    [ 'ext:algemeneNieuwsbrief' ], // newsletter
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
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
          ?meeting a besluit:Vergaderactiviteit ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${distributor.sourceGraph}> {
          ${decisionsReleaseFilter(distributor.releaseOptions.validateDecisionsRelease)}
          ?meeting ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

async function collectPublicationActivities(distributor) {
  const properties = [
    [ '^ext:internalDecisionPublicationActivityUsed' ],
    [ '^ext:internalDocumentPublicationActivityUsed' ],
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
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
          ?meeting a besluit:Vergaderactiviteit ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${distributor.sourceGraph}> {
          ?meeting ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

export {
  collectMeetings,
  collectReleasedNewsletter,
  collectPublicationActivities
}
