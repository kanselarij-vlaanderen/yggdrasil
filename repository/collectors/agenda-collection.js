import chunk from 'lodash.chunk';
import { updateTriplestore } from '../triplestore';
import { VALUES_BLOCK_SIZE } from '../../config';
import { DESIGN_AGENDA_STATUS } from '../../constants';

/**
 * Helpers to collect data about:
 * - agendas
 * - agendaitems
 * - agenda-activities
 * - submission-activities
 */

/*
 * Collect a set of given agenda's from the distributor's source graph in the temp graph.
 * The `agendaUris` to collect are passed in the options object.
 * If `isInitialDistribution` option is set, all agenda's are collected,
 * regardless of the `agendaUris` option.
 *
 * Agendas are only copied if they are not in the design status anymore.
 * I.e. triple ?agenda besluitvorming:agendaStatus <${DESIGN_AGENDA_STATUS}> doesn't exist.
 *
 * TODO: the filter on 'DESIGN' status may need to be removed to support
 * scenarios where an agenda changes back from 'APPROVED' to 'DESIGN' state.
 * To be clarified with business how often this scenario occurs and
 * what that means in terms of already published data.
 */
async function collectReleasedAgendas(distributor, options) {
  const collectAgendaQuery = function(agendaUris = []) {
    let agendaValues = '';
    if (agendaUris && agendaUris.length) {
      const values = agendaUris.map(uri => `<${uri}>`).join('\n');
      agendaValues = `VALUES ?agenda {
        ${values}
      }`;
    }

    return `
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT {
        GRAPH <${distributor.tempGraph}> {
          ?agenda a besluitvorming:Agenda ;
                  ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${distributor.sourceGraph}> {
          ?agenda a besluitvorming:Agenda .
          ${agendaValues}
          FILTER NOT EXISTS {
            ?agenda besluitvorming:agendaStatus <${DESIGN_AGENDA_STATUS}> .
          }
        }
      }`;
  };

  if (options.isInitialDistribution) {
    const queryString = collectAgendaQuery();
    await updateTriplestore(queryString);
  } else {
    const batches = chunk(options.agendaUris, VALUES_BLOCK_SIZE);
    for (const batch of batches) {
      const queryString = collectAgendaQuery(batch);
      await updateTriplestore(queryString);
    }
  }
}

/*
 * Collect related agendaitems for the relevant agenda's
 * from the distributor's source graph in the temp graph.
 *
 * Note: the 'formally OK' status is not taken into account on purpose.
 * The status is only an internal workflow tool for the secretary.
 * On approved agendas all agendaitems are inherently considered to be
 * in a publishable/approved state.
 * The application ensures on approval of an agenda that:
 * - new agendaitems that are not formally OK are moved to the next design agenda
 *   and not part of the approved agenda anymore
 * - modified agendaitems that are not formally OK anymore are moved to the next
 *   design agenda and the version on the approved agenda is rollbacked to the
 *   latest approved version of the agendaitem.
 */
async function collectReleasedAgendaitems(distributor) {
  const relatedAgendaitemsQuery = `
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX dct: <http://purl.org/dc/terms/>
      INSERT {
        GRAPH <${distributor.tempGraph}> {
          ?s a besluit:Agendapunt ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${distributor.tempGraph}> {
          ?agenda a besluitvorming:Agenda ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${distributor.sourceGraph}> {
          ?agenda dct:hasPart ?s .
          ?s a besluit:Agendapunt .
        }
      }`;
  await updateTriplestore(relatedAgendaitemsQuery);
}

/*
 * Collect related agenda-activities and submission-activities for the relevant agendaitems
 * from the distributor's source graph in the temp graph.
 */
async function collectAgendaitemActivities(distributor) {
  const properties = [
    [ '^besluitvorming:genereertAgendapunt' ], // agenda-activity
    [ '^besluitvorming:genereertAgendapunt', 'prov:wasInformedBy' ], // submission-activity
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
          ?agendaitem a besluit:Agendapunt ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${distributor.sourceGraph}> {
          ?agendaitem ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

export {
  collectReleasedAgendas,
  collectReleasedAgendaitems,
  collectAgendaitemActivities
}
