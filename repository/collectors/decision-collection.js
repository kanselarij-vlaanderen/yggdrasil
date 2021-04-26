import { updateTriplestore } from '../triplestore';

/**
 * Helpers to collect data about:
 * - agendaitem-treatments / decisions
 * - newsletters
 */

/*
 * Collect related agendaitem-treatments for the relevant agendaitems
 * from the distributor's source graph in the temp graph.
 *
 * Agendaitem-treatments are only copied if decisions of the meeting have already been released
 * I.e. triple ?meeting ext:releasedDecisions ?decisionReleaseDate exists
 */
async function collectReleasedAgendaitemTreatments(distributor) {
  const properties = [
    [ '^besluitvorming:heeftOnderwerp' ] // agendaitem-treatment
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
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
          ?agenda besluitvorming:isAgendaVoor ?meeting .
          ?meeting ext:releasedDecisions ?decisionReleaseDate .
          ?agendaitem ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

/*
 * Collect related newsitems for the relevant agendaitem-treatments
 * from the distributor's source graph in the temp graph.
 *
 * Newsitems are only copied if they have already been published
 * I.e. triple ?meeting ext:heeftMailCampagnes / ext:isVerstuurdOp ?sentMailDate . exists
 */
async function collectReleasedNewsitems(distributor) {
  const properties = [
    [ 'prov:generated' ], // newsitem
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
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
          ?agenda besluitvorming:isAgendaVoor ?meeting .
          ?meeting ext:heeftMailCampagnes / ext:isVerstuurdOp ?sentMailDate .
          ?treatment ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

export {
  collectReleasedAgendaitemTreatments,
  collectReleasedNewsitems
}
