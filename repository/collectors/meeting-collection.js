import { updateTriplestore } from '../triplestore';
import { decisionsReleaseFilter, documentsReleaseFilter } from './release-validations';

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
  // TODO KAS-3431 do we still need newsletter on meeting? seems pointless without the 2 "released" dates removed. Could be wrong
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

async function collectInternalDocumentsPublication(distributor) {
  const properties = [
    [ '^ext:internalDocumentPublicationActivityUsed' ], // internal document publication activity
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
          ${documentsReleaseFilter(distributor.releaseOptions.validateDocumentsRelease)}
          ?meeting ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

export {
  collectMeetings,
  collectReleasedNewsletter,
  collectInternalDocumentsPublication
}
