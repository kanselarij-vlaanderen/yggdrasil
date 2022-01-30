import { updateTriplestore } from '../triplestore';

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
 * If 'validateDocumentsRelease' is enabled on the distributor's release options
 * documents are only copied if the documents of the meeting have already been released.
 *
 * Some pieces are always visible, regardless of the documents release
 *
 * Note, all pieces (dossier:Stuk) are copied. Restrictions regarding visibility (access level, confidentiality)
 * are only taken into account at the level of a file (nfo:FileDataObject)
 */
async function collectReleasedDocuments(distributor) {
  const releasedDocumentPaths = [
    { type: 'besluit:Agendapunt', predicate: 'besluitvorming:geagendeerdStuk' },
    // TODO: KAS-1420: ext:documentenVoorBeslissing zou eventueel na bevestiging weg mogen. te bekijken.
    { type: 'besluit:BehandelingVanAgendapunt', predicate: 'ext:documentenVoorBeslissing' },
    { type: 'besluit:BehandelingVanAgendapunt', predicate: 'besluitvorming:genereertVerslag' },
    { type: 'besluitvorming:NieuwsbriefInfo', predicate: 'ext:documentenVoorPublicatie' },
    { type: 'ext:Indieningsactiviteit', predicate: 'prov:generated' },
    { type: 'dossier:Dossier', predicate: 'dossier:Dossier.bestaatUit' }
  ];

  let releaseDateFilter = '';
  if (distributor.releaseOptions.validateDocumentsRelease) {
    releaseDateFilter = '?agenda besluitvorming:isAgendaVoor / ext:releasedDocuments ?documentsReleaseDate .';
  }

  for (let path of releasedDocumentPaths) {
    const releasedDocumentsQuery = `
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
        PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
        PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
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
            ${releaseDateFilter}
            ?s ${path.predicate} ?piece .
            ?piece a dossier:Stuk .
          }
        }`;
    await updateTriplestore(releasedDocumentsQuery);
  }

  // pieces that are always visible, regardless of official documents release
  const piecePaths = [
    { type: 'besluit:Agendapunt', predicate: 'ext:bevatReedsBezorgdAgendapuntDocumentversie' },
    { type: 'dossier:Procedurestap', predicate: 'ext:bevatReedsBezorgdeDocumentversie' },
    { type: 'besluit:Vergaderactiviteit', predicate: 'ext:zittingDocumentversie' },
    { type: 'besluit:Vergaderactiviteit', predicate: 'dossier:genereert' }
  ];
  for (let path of piecePaths) {
    const piecesQuery = `
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
        PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
        PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
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
            ?s ${path.predicate} ?piece .
            ?piece a dossier:Stuk .
          }
        }`;
    await updateTriplestore(piecesQuery);
  }
}

/*
 * Collect related document-containers for the relevant pieces
 * from the distributor's source graph in the temp graph.
 */
async function collectDocumentContainers(distributor) {
  const properties = [
    [ '^dossier:collectie.bestaatUit' ], // document-container
  ];
  const path = properties.map(prop => prop.join(' / ')).map(path => `( ${path} )`).join(' | ');

  const relatedQuery = `
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
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
          ?piece ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

/*
 * Collect all physical files related to the 'virtual' files
 * from the distributor's source graph in the temp graph.
 *
 * Note, file visibility (confidentiality, access level) is checked
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
          ?virtualFile ${path} ?s .
          ?s a ?type .
        }
      }`;
  await updateTriplestore(relatedQuery);
}

export {
  collectReleasedDocuments,
  collectDocumentContainers,
  collectPhysicalFiles
}
