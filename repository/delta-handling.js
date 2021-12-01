import { ADMIN_GRAPH } from '../constants';
import { LOG_DELTA_PROCESSING } from '../config';
import { querySudo } from './auth-sudo';
import ModelCache from './model-cache';

const modelCache = new ModelCache();

/**
 * Returns a unique list of resource URIs that are a subject/object
 * in one of the changeset's triples.
*/
function reduceChangesets(delta) {
  const uriSet = new Set();
  for (let changeset of delta) {
    const triples = [...changeset.inserts, ...changeset.deletes];
    const subjects = triples.map(t => t.subject.value);
    const objects = triples.filter(t => t.object.type == 'uri').map(t => t.object.value);
    [...subjects, ...objects].forEach(uri => uriSet.add(uri));
  }

  const uris =  [...uriSet];
  if (LOG_DELTA_PROCESSING)
    console.log(`Reduced delta cache to ${uris.length} subjects:\n${uris.join('\n')}`);

  return uris;
}

/**
 * Fetches the related agendas for a given list of subjects URIs from a triplestore.
 * The related agenda(s) are fetched based on the subject's type and the configured model.
*/
async function fetchRelatedAgendas(subjects) {
  let agendas = new Set();

  if (subjects && subjects.length) {
    const typeMap = await constructSubjectsTypeMap(subjects);
    for (let typeUri in typeMap) {
      const subjectsForType = typeMap[typeUri];
      const agendasForType = await fetchRelatedAgendasForType(subjectsForType, typeUri);
      if (LOG_DELTA_PROCESSING)
        console.log(`Reduced ${subjectsForType.length} subjects of type <${typeUri}> to ${agendasForType.size} agendas `);
      agendas = new Set([...agendasForType, ...agendas]);
    }
  }

  if (LOG_DELTA_PROCESSING)
    console.log(`Reduced ${subjects.length} subjects to ${agendas.length} agendas `);

  return [...agendas];
}

/**
 * Constructs a type map for a given list of subjects by querying each subject's type.
 * The type map returned is a mapping from type URIs to an array of subject URIs like:
 * {
 *   "http://data.vlaanderen.be/ns/besluit#Agendapunt": ['uri-1', 'uri-2'],
 *   "http://data.vlaanderen.be/ns/besluit#BehandelingVanAgendapunt": ['uri-3'],
 *   ...
 * }
 *
 * @private
*/
async function constructSubjectsTypeMap(subjects) {
  const typeMap = {};
  const values = subjects.map(uri => `<${uri}>`).join('\n');    // TODO batch queries

  const subjectTypesQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX dbpedia: <http://dbpedia.org/ontology/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      PREFIX schema: <http://schema.org>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>

      SELECT DISTINCT ?s ?type WHERE {
        GRAPH <${ADMIN_GRAPH}> {
          VALUES ?s {
            ${values}
          }
          ?s a ?type .
        }
      }`;
  const result = await querySudo(subjectTypesQuery);
  result.results.bindings.forEach(binding => {
    const typeUri = binding['type'].value;
    if (!typeMap[typeUri])
      typeMap[typeUri] = [];
    typeMap[typeUri].push(binding['s'].value);
  });

  return typeMap;
}

/**
 * Fetches a unique set of related agendas for a given list of subjects URIs.
 * The related agenda(s) are fetched based on the given type and the configured model.
 *
 * @private
*/
async function fetchRelatedAgendasForType(subjects, typeUri) {
  const agendas = new Set();

  const paths = modelCache.getPathsFromAgenda(typeUri);

  if (paths != null) {
    let pathToAgendaStatement = '';
    if (paths.length == 0) { // typeUri == besluitvorming:Agenda
      pathToAgendaStatement = `BIND(?s as ?agenda) .`;
    } else {
      const queryPath = paths.map(path => path.join(' / ')).map(path => `( ${path} )`).join(' | ');
      pathToAgendaStatement = `?agenda ${queryPath} ?s .`;
    }

    const values = subjects.map(uri => `<${uri}>`).join('\n'); // TODO batch queries
    const agendaQuery = `
          PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
          PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
          PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
          PREFIX dbpedia: <http://dbpedia.org/ontology/>
          PREFIX dct: <http://purl.org/dc/terms/>
          PREFIX prov: <http://www.w3.org/ns/prov#>
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          PREFIX schema: <http://schema.org>
          PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>

          SELECT DISTINCT ?agenda WHERE {
            GRAPH <${ADMIN_GRAPH}> {
              VALUES ?s {
                ${values}
              }
              ?s a <${typeUri}> .
              ${pathToAgendaStatement}
              ?agenda a besluitvorming:Agenda .
            }
          }`;
    // TODO add additional filter on agenda status to exclude agendas in design state
    // See FILTER NOT EXISTS in ./collectors/agenda-collection

    const result = await querySudo(agendaQuery);
    result.results.bindings.forEach(b => agendas.add(b['agenda'].value));
  }
  // else: this type is not configured in the model, hence not relevant to any agenda

  return agendas;
}

export {
  reduceChangesets,
  fetchRelatedAgendas
}
