import { ADMIN_GRAPH } from '../config';
import { querySudo } from './auth-sudo';
import ModelCache from './model-cache';

const modelCache = new ModelCache();

function reduceChangesets(delta) {
  const uriSet = new Set();
  for (let changeset of delta) {
    const triples = [...changeset.inserts, ...changeset.deletes];
    const subjects = triples.map(t => t.subject.value);
    const objects = triples.filter(t => t.object.type == 'uri').map(t => t.object.value);
    [...subjects, ...objects].forEach(uri => uriSet.add(uri));
  }
  return [...uriSet];
}

async function fetchRelatedAgendas(subjects) {
  const agendas = new Set();
  if (subjects && subjects.length) {
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
      const typeUri = binding['type'];
      if (!typeMap[typeUri])
        typeMap[typeUri] = [];

      typeMap[typeUri].push(binding['s']);
    });

    for (let typeUri in typeMap) {
      const paths = modelCache.getPathsFromAgenda(typeUri);
      const queryPath = paths.map(path => path.map(prop => prop.join(' / '))).map(path => `( ${path} )`).join(' | ');
      const subjects = typeMap[type].map(uri => `<${uri}>`).join('\n'); // TODO batch queries

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
            ${subjects}
          }
          ?s a <${typeUri}> .
          ?agenda a besluitvorming:Agenda ;
            ${queryPath} ?s .
        }
      }`;

      const result = await querySudo(agendaQuery);
      result.results.bindings.forEach(b => agendas.add(b['agenda'].value));
    }
  }

  return [...agendas];
}

export {
  reduceChangesets,
  fetchRelatedAgendas
}
