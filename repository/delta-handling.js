import { ADMIN_GRAPH } from '../constants';
import { LOG_DELTA_PROCESSING, VALUES_BLOCK_SIZE } from '../config';
import { querySudo } from './auth-sudo';
import ModelCache from './model-cache';
import chunk from 'lodash.chunk';

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
  if (LOG_DELTA_PROCESSING) {
    const uriString = uris.map(u => `- <${u}>`).join('\n');
    console.log(`Reduced delta cache to ${uris.length} subjects:\n${uriString}`);
  }

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
    console.log(`Summary: reduced ${subjects.length} subjects to ${agendas.size} agendas `);

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

  const collectSubjectQuery = function(subjectUris = []) {
    let subjectValues = '';
    if (subjectUris && subjectUris.length) {
      const values = subjectUris.map(uri => `<${uri}>`).join('\n');
      subjectValues = `VALUES ?s {
        ${values}
      }`;
    }

    return `
      SELECT DISTINCT ?s ?type WHERE {
        GRAPH <${ADMIN_GRAPH}> {
          ${subjectValues}
          ?s a ?type .
        }
      }`;
  };

  const batches = chunk(subjects, VALUES_BLOCK_SIZE);
  for (const batch of batches) {
    const subjectTypesQuery = collectSubjectQuery(batch);
    const result = await querySudo(subjectTypesQuery);
    result.results.bindings.forEach(binding => {
      const typeUri = binding['type'].value;
      if (!typeMap[typeUri]) {
        typeMap[typeUri] = [];
      }
      typeMap[typeUri].push(binding['s'].value);
    });
  }

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

    const collectAgendaQuery = function(typeUris = []) {
      let typeValues = '';
      if (typeUris && typeUris.length) {
        const values = typeUris.map(uri => `<${uri}>`).join('\n');
        typeValues = `VALUES ?s {
          ${values}
        }`;
      }

      return `
        ${modelCache.getSparqlPrefixes()}
        SELECT DISTINCT ?agenda WHERE {
          GRAPH <${ADMIN_GRAPH}> {
            ${typeValues}
            ?s a <${typeUri}> .
            ${pathToAgendaStatement}
            ?agenda a besluitvorming:Agenda .
          }
        }`;
    };

    const batches = chunk(subjects, VALUES_BLOCK_SIZE);
    for (const batch of batches) {
      const agendaQuery = collectAgendaQuery(batch);
      const result = await querySudo(agendaQuery);
      result.results.bindings.forEach(b => agendas.add(b['agenda'].value));
    }
  }
  // else: this type is not configured in the model, hence not relevant to any agenda

  return agendas;
}

export {
  reduceChangesets,
  fetchRelatedAgendas
}
