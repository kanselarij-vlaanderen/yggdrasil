import { updateTriplestore } from '../triplestore';
import { decisionsReleaseFilter, documentsReleaseFilter, ratificationsReleaseFilter } from './release-validations';

/**
 * Helpers to collect data about:
 * - pieces (dossier:Stuk)
 * - document-containers (dossier:Serie)
 * - files (nfo:FileDataObject)
 */

/*
 * Collect related pieces for any of the relevant resources
 * from the distributor's source graph in the temp graph.
 *
 * If 'validateDocumentsRelease' and/or 'validateDecisionsRelease' is enabled
 * on the distributor's release options documents are only copied if the
 * documents/decisions of the meeting have already been released.
 *
 * Some pieces are always visible, regardless of the documents/decision release
 *
 * Note, all pieces (dossier:Stuk) are copied. Restrictions regarding visibility (access level)
 * are only taken into account at the level of a file (nfo:FileDataObject)
 */
async function collectReleasedDocuments(distributor) {
  // note: this release filter only allows distribution of documents that are connected to an agendaitem
  const documentsFilter = documentsReleaseFilter(
    distributor.releaseOptions.validateDocumentsRelease,
    distributor.releaseOptions.validateDecisionResults
  );
  // note: this release filter only allows distribution of ratification documents of subcases that have a connecton to an agenda
  const ratificationsFilter = ratificationsReleaseFilter(distributor.releaseOptions.validateDocumentsRelease)

  const decisionsFilter = decisionsReleaseFilter(distributor.releaseOptions.validateDecisionsRelease);

  const releasedPiecePaths = [
    // pieces only visible if documents have been released
    { type: 'besluit:Agendapunt', predicate: 'besluitvorming:geagendeerdStuk', filter: documentsFilter },
    // check resource files for comments on this predicate
    // { type: 'besluit:BehandelingVanAgendapunt', predicate: 'prov:used', filter: documentsFilter },
    { type: 'ext:Nieuwsbericht', predicate: 'besluitvorming:heeftBijlage', filter: documentsFilter },
    { type: 'ext:Indieningsactiviteit', predicate: 'prov:generated', filter: documentsFilter },
    { type: 'dossier:Dossier', predicate: 'dossier:Dossier.bestaatUit', filter: documentsFilter },
    { type: 'dossier:Procedurestap', predicate: 'ext:heeftBekrachtiging', filter: ratificationsFilter },

    // pieces only visible if decisions have been released
    { type: 'besluitvorming:Beslissingsactiviteit', predicate: '^besluitvorming:beschrijft', filter: decisionsFilter },

    // pieces that are always visible, regardless of official documents release
    { type: 'besluit:Agendapunt', predicate: 'ext:bevatReedsBezorgdAgendapuntDocumentversie' },
    { type: 'dossier:Procedurestap', predicate: 'ext:bevatReedsBezorgdeDocumentversie' },
    { type: 'besluit:Vergaderactiviteit', predicate: 'ext:zittingDocumentversie' },
    { type: 'besluit:Vergaderactiviteit', predicate: 'dossier:genereert' },
    { type: 'besluit:Vergaderactiviteit', predicate: 'besluitvorming:heeftNotulen' },

    // pieces that have been signed, requires other pieces
    { type: 'dossier:Stuk', predicate: '^sign:ongetekendStuk' },
    { type: 'dossier:Stuk', predicate: 'sign:getekendStukKopie' },
  ];

  for (let path of releasedPiecePaths) {
    const releasedDocumentsQuery = `
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
        PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
        PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX sign: <http://mu.semte.ch/vocabularies/ext/handtekenen/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        INSERT {
          GRAPH <${distributor.tempGraph}> {
            ?piece a dossier:Stuk ;
               ext:tracesLineageTo ?agenda .
          }
        } WHERE {
          GRAPH <${distributor.tempGraph}> {
            ?s a ${path.type} ;
                ext:tracesLineageTo ?agenda .
          }
          GRAPH <${distributor.sourceGraph}> {
            ${path.filter ? path.filter : ''}
            ?s a ${path.type} .
            ?s ${path.predicate} ?piece .
            ?piece a dossier:Stuk .
          }
        }`;
    await updateTriplestore(releasedDocumentsQuery);
  }
}

/*
 * Collect related document-containers for the relevant pieces
 * from the distributor's source graph in the temp graph.
 */
async function collectDocumentContainers(distributor) {
  const properties = [
    [ '^dossier:Collectie.bestaatUit' ], // document-container
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
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
          ?piece a dossier:Stuk .
          ?piece ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

/*
 * Collect all derived files related to the 'virtual' files
 * from the distributor's source graph in the temp graph.
 *
 * Note, file visibility (access level) is checked
 * at the level of the 'virtual' file.
 */
async function collectDerivedFiles(distributor) {
  const properties = [
    [ '^prov:hadPrimarySource' ], // derived-file (e.g. PDF file generated from a Word file)
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      INSERT {
        GRAPH <${distributor.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${distributor.tempGraph}> {
          ?virtualFile a nfo:FileDataObject ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${distributor.sourceGraph}> {
          ?virtualFile a nfo:FileDataObject .
          ?virtualFile ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

/*
 * Collect all physical files related to the 'virtual' files
 * from the distributor's source graph in the temp graph.
 *
 * Note, file visibility (access level) is checked
 * at the level of the 'virtual' file.
 */
async function collectPhysicalFiles(distributor) {
  const properties = [
    [ '^nie:dataSource' ], // physical-file
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      INSERT {
        GRAPH <${distributor.tempGraph}> {
          ?s a ?type ;
             ext:tracesLineageTo ?agenda .
        }
      } WHERE {
        GRAPH <${distributor.tempGraph}> {
          ?virtualFile a nfo:FileDataObject ;
              ext:tracesLineageTo ?agenda .
        }
        GRAPH <${distributor.sourceGraph}> {
          ?virtualFile a nfo:FileDataObject .
          ?virtualFile ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

export {
  collectReleasedDocuments,
  collectDocumentContainers,
  collectDerivedFiles,
  collectPhysicalFiles,
}
