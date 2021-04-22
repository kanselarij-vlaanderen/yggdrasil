import chunk from 'lodash.chunk';
import Distributor from '../distributor';
import {
  ADMIN_GRAPH,
  GOVERNMENT_GRAPH,
  DESIGN_AGENDA_STATUS,
  AGENDAITEM_FORMALLY_OK_STATUS,
  ACCESS_LEVEL_CABINET,
  VALUES_BLOCK_SIZE } from '../../config';
import { runStage } from '../timing';
import { queryTriplestore, updateTriplestore } from '../triplestore';

/**
 * Distributor for government (intern-overheid) profile
 */
export default class GovernmentDistributor extends Distributor {
  constructor() {
    super({
      sourceGraph: ADMIN_GRAPH,
      targetGraph: GOVERNMENT_GRAPH
    });
  }

  async collect(options) {
    await runStage('Collect agendas', async () => {
      await this.collectReleasedAgendas(options);
    }, this.constructor.name);

    await runStage('Collect meeting and agendaitems', async () => {
      await this.collectMeetings();
      await this.collectReleasedAgendaitems();
    }, this.constructor.name);

    await runStage('Collect meeting newsletter', async () => {
      await this.collectReleasedNewsletter();
    }, this.constructor.name);

    await runStage('Collect activities of agendaitems', async () => {
      await this.collectAgendaitemActivities();
    }, this.constructor.name);

    await runStage('Collect subcases and cases', async () => {
      await this.collectSubcasesAndCases();
    }, this.constructor.name);

    await runStage('Collect released and approved decisions/treatments', async () => {
      await this.collectReleasedAgendaitemTreatments();
    }, this.constructor.name);

    await runStage('Collect newsitems', async () => {
      await this.collectReleasedNewsitems();
    }, this.constructor.name);

    await runStage('Collect released documents', async () => {
      await this.collectReleasedDocuments();
    }, this.constructor.name);

    await runStage('Collect visible files', async () => {
      await this.collectVisibleFiles();
    }, this.constructor.name);
  }

  /*
   * Agendas are only copied if they are not in the design status anymore
   * I.e. triple ?agenda besluitvorming:agendaStatus <${DESIGN_AGENDA_STATUS}> doesn't exist
  */
  async collectReleasedAgendas(options) {
    if (options.isInitialDistribution) {
      const queryString = this.collectAgendaQuery();
      await updateTriplestore(queryString);
    } else {
      const batches = chunk(options.agendaUris, VALUES_BLOCK_SIZE);
      for (const batch of batches) {
        const queryString = this.collectAgendaQuery(batch);
        await updateTriplestore(queryString);
      }
    }
  }

  collectAgendaQuery(agendaUris = []) {
    let agendaValues = '';
    if (agendaUris && agendaUris.length) {
      const values = agendaUris.map(uri => `<${uri}>`).join('\n');
      agendaValues = `VALUES ?agenda {
        ${values}
      }`;
    }

    return `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?agenda a besluitvorming:Agenda ;
                  ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.sourceGraph}> {
          ?agenda a besluitvorming:Agenda .
          ${agendaValues}
          FILTER NOT EXISTS {
            ?agenda besluitvorming:agendaStatus <${DESIGN_AGENDA_STATUS}> .
          }
        }
      }`;
  }

  async collectMeetings() {
    const properties = [
      [ 'besluitvorming:isAgendaVoor' ], // meeting
      [ '^besluitvorming:behandelt' ], // meeting
    ];
    const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

    const relatedQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.tempGraph}> {
          ?agenda a besluitvorming:Agenda ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.sourceGraph}> {
          ?agenda ${path} ?s .
          ?s a ?type .
        }
      }`;
    await updateTriplestore(relatedQuery);
  }

  /*
   * Newsletters are only copied if they have already been published
   * I.e. triple ?meeting ext:heeftMailCampagnes / ext:isVerstuurdOp ?sentMailDate . exists
  */
  async collectReleasedNewsletter() {
    const properties = [
      [ 'ext:algemeneNieuwsbrief' ], // newsletter
    ];
    const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

    const relatedQuery = `
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.tempGraph}> {
          ?meeting a besluit:Vergaderactiviteit ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.sourceGraph}> {
          ?meeting ext:heeftMailCampagnes / ext:isVerstuurdOp ?sentMailDate .
          ?meeting ${path} ?s .
          ?s a ?type .
        }
      }`;
    await updateTriplestore(relatedQuery);
  }

  /*
   * Agendaitems are only copied if they are marked as 'formally OK'
   * or don't have any formally OK status (legacy data)
   * I.e. triple ?agendaitem ext:formeelOK <${AGENDAITEM_FORMALLY_OK_STATUS}> exists
  */
  async collectReleasedAgendaitems() {
    const relatedAgendaitemsQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX dct: <http://purl.org/dc/terms/>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?s a besluit:Agendapunt ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.tempGraph}> {
          ?agenda a besluitvorming:Agenda ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.sourceGraph}> {
          ?agenda dct:hasPart ?s .
          {
            ?s a besluit:Agendapunt .
            FILTER NOT EXISTS {
              ?s ext:formeelOK ?anyStatus .
            }
          }
          UNION
          {
            ?s a besluit:Agendapunt .
            ?s ext:formeelOK <${AGENDAITEM_FORMALLY_OK_STATUS}> .
          }
        }
      }`;
    await updateTriplestore(relatedAgendaitemsQuery);
  }

  async collectAgendaitemActivities() {
    const properties = [
      [ '^besluitvorming:genereertAgendapunt' ], // agenda-activity
      [ '^besluitvorming:genereertAgendapunt', 'prov:wasInformedBy' ], // submission-activity
    ];
    const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

    const relatedQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.tempGraph}> {
          ?agendaitem a besluit:Agendapunt ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.sourceGraph}> {
          ?agendaitem ${path} ?s .
          ?s a ?type .
        }
      }`;
    await updateTriplestore(relatedQuery);
  }

  /*
   * Note, all subcases and cases are copied. Restrictions regarding visibility
   * because of confidentiality are only taken into account
   * at the level of a file (nfo:FileDataObject)
  */
  async collectSubcasesAndCases() {
    const properties = [
      [ '^besluitvorming:genereertAgendapunt', 'besluitvorming:vindtPlaatsTijdens' ], // subcase
      [ '^besluitvorming:genereertAgendapunt', 'besluitvorming:vindtPlaatsTijdens', '^dossier:doorloopt' ] // case
    ];
    const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

    const relatedQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.tempGraph}> {
          ?agendaitem a besluit:Agendapunt ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.sourceGraph}> {
          ?agendaitem ${path} ?s .
          ?s a ?type .
        }
      }`;
    await updateTriplestore(relatedQuery);
  }

  /*
   * Agendaitem-treatments are only copied if decisions of the meeting have already been released
   * I.e. triple ?meeting ext:releasedDecisions ?decisionReleaseDate exists
  */
  async collectReleasedAgendaitemTreatments() {
    const properties = [
      [ '^besluitvorming:heeftOnderwerp' ] // agendaitem-treatment
    ];
    const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

    const relatedQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.tempGraph}> {
          ?agendaitem a besluit:Agendapunt ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.sourceGraph}> {
          ?agenda besluitvorming:isAgendaVoor ?meeting .
          ?meeting ext:releasedDecisions ?decisionReleaseDate .
          ?agendaitem ${path} ?s .
          ?s a ?type .
        }
      }`;
    await updateTriplestore(relatedQuery);
  }

  /*
   * Newsitems are only copied if they have already been published
   * I.e. triple ?meeting ext:heeftMailCampagnes / ext:isVerstuurdOp ?sentMailDate . exists
  */
  async collectReleasedNewsitems() {
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
        GRAPH <${this.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.tempGraph}> {
          ?treatment a besluit:BehandelingVanAgendapunt ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.sourceGraph}> {
          ?agenda besluitvorming:isAgendaVoor ?meeting .
          ?meeting ext:heeftMailCampagnes / ext:isVerstuurdOp ?sentMailDate .
          ?treatment ${path} ?s .
          ?s a ?type .
        }
      }`;
    await updateTriplestore(relatedQuery);
  }

  /*
   * Collect all documents related to any of the previously copied visible items if the documents have been released.
   * I.e. triple ?meeting ext:releasedDocuments ?date . exists
   * Some documents are always visible, regardless of the documents release
   * Note, all documents (dossier:Stuk) are copied. Restrictions regarding visibility (access level, confidentiality)
   * are only taken into account at the level of a file (nfo:FileDataObject)
  */
  async collectReleasedDocuments() {
    const releasedDocumentPaths = [
      { type: 'besluit:Agendapunt', predicate: 'besluitvorming:geagendeerdStuk' },
      { type: 'besluit:BehandelingVanAgendapunt', predicate: 'ext:documentenVoorBeslissing' },
      { type: 'besluit:BehandelingVanAgendapunt', predicate: 'besluitvorming:genereertVerslag' },
      { type: 'besluitvorming:NieuwsbriefInfo', predicate: 'ext:documentenVoorPublicatie' },
      { type: 'ext:Indieningsactiviteit', predicate: 'prov:generated' },
      { type: 'dossier:Dossier', predicate: 'dossier:Dossier.bestaatUit' }
    ];
    for (let path of releasedDocumentPaths) {
      const releasedDocumentsQuery = `
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
        PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
        PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        INSERT {
          GRAPH <${this.tempGraph}> {
            ?document a dossier:Stuk ;
               ext:tracesLineageTo ?agenda .
          }
        } WHERE {
          GRAPH <${this.tempGraph}> {
            ?s a ${path.type} ;
                ext:tracesLineageTo ?agenda .
          }
          GRAPH <${this.sourceGraph}> {
            ?agenda besluitvorming:isAgendaVoor ?meeting .
            ?meeting ext:releasedDocuments ?date .
            ?s ${path.predicate} ?document .
            ?document a dossier:Stuk .
          }
        }`;
      await updateTriplestore(releasedDocumentsQuery);
    }

    // documents that are always visible, regardless of official documents release
    const documentPaths = [
      { type: 'besluit:Agendapunt', predicate: 'ext:bevatReedsBezorgdAgendapuntDocumentversie' },
      { type: 'dossier:Procedurestap', predicate: 'ext:bevatReedsBezorgdeDocumentversie' },
      { type: 'besluit:Vergaderactiviteit', predicate: 'ext:zittingDocumentversie' },
      { type: 'besluit:Vergaderactiviteit', predicate: 'dossier:genereert' }
    ];
    for (let path of documentPaths) {
      const documentsQuery = `
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
        PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
        PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        INSERT {
          GRAPH <${this.tempGraph}> {
            ?document a dossier:Stuk ;
               ext:tracesLineageTo ?agenda .
          }
        } WHERE {
          GRAPH <${this.tempGraph}> {
            ?s a ${path.type} ;
                ext:tracesLineageTo ?agenda .
          }
          GRAPH <${this.sourceGraph}> {
            ?s ${path.predicate} ?document .
            ?document a dossier:Stuk .
          }
        }`;
      await updateTriplestore(documentsQuery);
    }

    const documentContainerQuery = `
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?container a dossier:Serie ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.tempGraph}> {
          ?document a dossier:Stuk ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.sourceGraph}> {
          ?container dossier:collectie.bestaatUit ?document .
        }
      }`;
    await updateTriplestore(documentContainerQuery);
  }

  /*
   * Collect all files related to any of the previously copied released documents
   * that are accessible for the government-profile
   * I.e. the document is not confidential and doesn't have access level 'Intern regering'
   * nor is it linked to a confidential case/subcase
  */
  async collectVisibleFiles() {
    const visibleFileQuery = `
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?file a nfo:FileDataObject ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.tempGraph}> {
          ?document a dossier:Stuk ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.sourceGraph}> {
          ?document ext:file ?file .
          FILTER NOT EXISTS {
            ?document ext:vertrouwelijk "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
          }
          FILTER NOT EXISTS {
            ?document ^prov:generated / ext:indieningVindtPlaatsTijdens / dossier:doorloopt? ?subcase .
            ?subcase ext:vertrouwelijk "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
          }
          FILTER NOT EXISTS {
            ?document ext:toegangsniveauVoorDocumentVersie <${ACCESS_LEVEL_CABINET}> .
          }
        }
      }`;
    await updateTriplestore(visibleFileQuery);

    const physicalFileQuery = `
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?file a nfo:FileDataObject ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${this.tempGraph}> {
          ?virtualFile a nfo:FileDataObject ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.sourceGraph}> {
          ?file a nfo:FileDataObject ;
            nie:dataSource ?virtualFile .
        }
      }`;
    await updateTriplestore(physicalFileQuery);
  }
}
