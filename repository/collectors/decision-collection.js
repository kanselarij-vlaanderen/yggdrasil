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
 * If 'validateDecisionsRelease' is enabled on the distributor's release options
 * agendaitem-treatments are only copied if decisions of the meeting have already been released.
 */
async function collectReleasedAgendaitemTreatments(distributor) {
  const properties = [
    [ '^besluitvorming:heeftOnderwerp' ] // agendaitem-treatment
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  let releaseDateFilter = '';
  if (distributor.releaseOptions.validateDecisionsRelease) {
    releaseDateFilter = '?agenda besluitvorming:isAgendaVoor / ext:releasedDecisions ?decisionReleaseDate .';
  }

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
          ${releaseDateFilter}
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
 * If 'validateNewsitemsRelease' is enabled on the distributor's release options
 * newsitems are only copied if they have already been published.
 */
async function collectReleasedNewsitems(distributor) {
  const properties = [
    [ 'prov:generated' ], // newsitem
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  let releaseDateFilter = '';
  if (distributor.releaseOptions.validateNewsitemsRelease) {
    releaseDateFilter = '?agenda besluitvorming:isAgendaVoor / ext:heeftMailCampagnes / ext:isVerstuurdOp ?sentMailDate .';
  }

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
          ${releaseDateFilter}
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