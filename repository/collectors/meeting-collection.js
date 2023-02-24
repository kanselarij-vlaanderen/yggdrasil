import { updateTriplestore } from '../triplestore';

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
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
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

async function collectPublicationActivities(distributor) {
  const properties = [
    [ '^ext:internalDecisionPublicationActivityUsed' ],
    [ '^ext:internalDocumentPublicationActivityUsed' ],
    [ '^prov:used' ], // themis-publication-activity
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
  collectPublicationActivities
}
