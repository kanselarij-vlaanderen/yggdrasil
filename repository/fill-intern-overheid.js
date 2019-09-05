import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import moment from 'moment';
mu.query = querySudo;

import { removeInfoNotInTemp, notConfidentialFilter, addRelatedFiles,
  cleanup, fillOutDetailsOnVisibleItems, addRelatedToAgendaItemAndSubcase,
  notBeperktOpenbaarFilter, notInternOverheidFilter, logStage,
  removeThingsWithLineageNoLongerInTemp, filterAgendaMustBeInSet
} from './helpers';

// logic is: make visible if openbaarheid is ok AND
// if has accepted decision with agenda date > last date
const sessionPublicationDateHasPassed = function(){
  return `
    ?s besluit:isAangemaaktVoor ?session .
    ?session ext:decisionPublicationDate ?date .
    FILTER(?date < "${moment().utc().toISOString()}"^^xsd:dateTime )`;
};


const addVisibleAgendas = (queryEnv, extraFilters) => {
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a besluitvorming:Agenda.
      ?s ext:tracesLineageTo ?s.
    }
  } WHERE {
    GRAPH <${queryEnv.adminGraph}> {
      ?s a besluitvorming:Agenda.
      ?s ext:agendaNaam ?naam.
      FILTER(?naam != "Ontwerpagenda")

      ?s dct:hasPart ?item.
      ?subcase besluitvorming:isGeagendeerdVia ?item.
      ?subcase ext:procedurestapHeeftBesluit ?decision.
      ?decision besluitvorming:goedgekeurd "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
      
      ${extraFilters}
    }
  }`;
  return queryEnv.run(query);
};
const addAllRelatedToAgenda = (queryEnv, extraFilters) => {
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a ?thing .
      ?s ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?agenda a besluitvorming:Agenda .
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?s a ?thing .
      
      ?s a ?thing .
      { { ?s ?p ?agenda } 
        UNION 
        { ?agenda ?p ?s } 
        UNION
        { ?agenda dct:hasPart ?agendaItem .
          ?s besluitvorming:isGeagendeerdVia ?agendaItem .
        }
      }
      
      ${extraFilters}
      
      FILTER( ?thing NOT IN(besluitvorming:Agenda) )
      
      FILTER NOT EXISTS {
        ?s a besluit:AgendaPunt .
        ?subcase ext:procedurestapHeeftBesluit ?decision.
        FILTER NOT EXISTS {
          ?decision besluitvorming:goedgekeurd "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
        }
      }
    }
  }`;
  return queryEnv.run(query);
};

const addAllRelatedDocuments = (queryEnv, extraFilters) => {
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a ?thing .
      ?version a ?subthing .
      ?s ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?decision a ?targetClass .
      ?decision ext:tracesLineageTo ?agenda .
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?s a ?thing .
      ?decision ?p ?s .
      ?decision a besluit:Besluit .
      ?decision besluitvorming:goedgekeurd "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
      
      FILTER( ?thing IN(
        foaf:Document,
        ext:DocumentVersie ) )

      ${extraFilters}

      OPTIONAL {
        ?s besluitvorming:heeftVersie ?version.
        ?version a ?subthing.
      }
    }
  }`;
  return queryEnv.run(query);
};

export const fillUp = async (queryEnv, agendas) => {
  try {
    const start = moment().utc();
    const filter = [notConfidentialFilter, notBeperktOpenbaarFilter].join("\n");
    const agendaFilter = filterAgendaMustBeInSet(agendas);
    const filterAgendasWithAccess=[
      notConfidentialFilter, notBeperktOpenbaarFilter,
      sessionPublicationDateHasPassed(),
      agendaFilter
    ].join("\n");
    let targetGraph = queryEnv.targetGraph;
    let stageStart = moment().utc();
    logStage(stageStart, `fill overheid started at: ${start}`, targetGraph);
    await addVisibleAgendas(queryEnv, filterAgendasWithAccess);
    logStage(stageStart, `overheid agendas added`, targetGraph);
    stageStart = moment().utc();
    await addAllRelatedToAgenda(queryEnv, filter);
    logStage(stageStart, `related to agenda added`, targetGraph);
    stageStart = moment().utc();
    await addRelatedToAgendaItemAndSubcase(queryEnv, filter);
    logStage(stageStart, `agenda items and subcases added`, targetGraph);
    stageStart = moment().utc();
    await addAllRelatedDocuments(queryEnv, filter);
    logStage(stageStart, 'documents added', targetGraph);
    stageStart = moment().utc();
    await addRelatedFiles(queryEnv);
    logStage(stageStart, 'related files added', targetGraph);
    stageStart = moment().utc();
    await fillOutDetailsOnVisibleItems(queryEnv);
    logStage(stageStart, 'details added', targetGraph);
    stageStart = moment().utc();
    await removeThingsWithLineageNoLongerInTemp(queryEnv, agendas);
    logStage(stageStart, 'lineage updated', targetGraph);
    if(queryEnv.fullRebuild){
      stageStart = moment().utc();
      await removeInfoNotInTemp(queryEnv);
      logStage(stageStart, 'removed info not in temp', targetGraph);
    }
    stageStart = moment().utc();
    await cleanup(queryEnv);
    logStage(stageStart, 'done filling overheid', targetGraph);
    const end = moment().utc();
    logStage(start,`fill overheid ended at: ${end}`, targetGraph);
  } catch (e) {
    logStage(e, queryEnv.targetGraph);
  }
};
