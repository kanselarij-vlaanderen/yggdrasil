import Distributor from '../distributor';
import { runStage } from '../timing';
import { updateTriplestore } from '../triplestore';
import { ADMIN_GRAPH, MINISTER_GRAPH, AGENDA_TYPE, ACCESS_LEVEL_SECRETARY } from '../../constants';
import { countResources } from '../query-helpers';
import {
  collectReleasedAgendas,
  collectReleasedAgendaitems,
  collectAgendaitemActivities
} from '../collectors/agenda-collection';
import {
  collectMeetings,
  collectPublicationActivities
} from '../collectors/meeting-collection';
import { collectCasesSubcasesAndDecisionmakingFlows } from '../collectors/case-collection';
import {
  collectReleasedAgendaitemTreatments,
  collectAgendaitemDecisionActivitiesAndNewsitems
} from '../collectors/decision-collection';
import {
  collectReleasedDocuments,
  collectDocumentContainers,
  collectPhysicalFiles,
  collectPrimarySourceFiles
} from '../collectors/document-collection';

/**
 * Distributor for minister profile
 */
export default class MinisterDistributor extends Distributor {
  constructor() {
    super({
      sourceGraph: ADMIN_GRAPH,
      targetGraph: MINISTER_GRAPH
    });

    // Ministers are allowed to see 'work in progress' on decisions and news-items
    // related to approved agendas. Hence, we don't validate on decision release date
    // and news-item release date

    this.releaseOptions = {
      validateDecisionsRelease: true,
      validateDocumentsRelease: false
    };
  }

  async collect(options) {
    await runStage('Collect agendas', async () => {
      await collectReleasedAgendas(this, options);
    }, this.constructor.name);


    const nbOfAgendas = await countResources({ graph: this.tempGraph, type: AGENDA_TYPE });

    if (nbOfAgendas) {
      await runStage('Collect meeting and agendaitems', async () => {
        await collectMeetings(this);
        await collectReleasedAgendaitems(this);
      }, this.constructor.name);

      await runStage('Collect publication activities of meeting', async () => {
        await collectPublicationActivities(this);
      }, this.constructor.name);

      await runStage('Collect activities of agendaitems', async () => {
        await collectAgendaitemActivities(this);
      }, this.constructor.name);

      await runStage('Collect subcases and cases', async () => {
        await collectCasesSubcasesAndDecisionmakingFlows(this);
      }, this.constructor.name);

      await runStage('Collect released agenda-item treatments', async () => {
        await collectReleasedAgendaitemTreatments(this);
      }, this.constructor.name);

      await runStage('Collect decision-activities and newsitems', async () => {
        await collectAgendaitemDecisionActivitiesAndNewsitems(this);
      }, this.constructor.name);

      await runStage('Collect released documents', async () => {
        await collectReleasedDocuments(this);
      }, this.constructor.name);

      await runStage('Collect document containers', async () => {
        await collectDocumentContainers(this);
      }, this.constructor.name);

      await runStage('Collect visible files', async () => {
        await this.collectVisibleFiles();
      }, this.constructor.name);

      await runStage('Collect physical files', async () => {
        await collectPhysicalFiles(this);
      }, this.constructor.name);

      await runStage('Collect primary source files', async() => {
        await collectPrimarySourceFiles(this);
      }, this.constructor.name);
    }

    return nbOfAgendas > 0;
  }

  /*
   * Collect all files related to any of the previously copied released documents
   * that are accessible for the minister-profile
   * I.e. the document does not have an access level 'Intern secretarie'.
  */
  async collectVisibleFiles() {
    const visibleFileQuery = `
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?file a nfo:FileDataObject ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.tempGraph}> {
          ?piece a dossier:Stuk ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.sourceGraph}> {
          ?piece ext:file ?file ;
                 besluitvorming:vertrouwelijkheidsniveau ?accessLevel .
          FILTER ( ?accessLevel != <${ACCESS_LEVEL_SECRETARY}> )
        }
      }`;
    await updateTriplestore(visibleFileQuery);
  }
}
