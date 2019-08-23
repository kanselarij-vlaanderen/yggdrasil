import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import moment from 'moment';
mu.query = querySudo;

import { removeInfoNotInTemp, notConfidentialFilter, addRelatedFiles,
  cleanup, fillOutDetailsOnVisibleItems, addRelatedToAgendaItemAndSubcase,
  notBeperktOpenbaarFilter, notInternOverheidFilter, logStage} from './helpers';

// logic is: make visible if openbaarheid is ok AND
// if has accepted decision with agenda date > last date
const sessionPublicationDateHasPassed = function(){
  return `
    ?s besluit:isAangemaaktVoor ?session .
    ?session ext:decisionPublicationDate ?date .
    FILTER(?date < "${moment().utc().toISOString()}"^^xsd:dateTime )`;
};


const addVisibleAgendas = (queryEnv, extraFilters) => {
  // TODO can reduce the number of agendas examined using delta service
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a besluitvorming:Agenda.
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
    }
    GRAPH <${queryEnv.agendaLineageGraph}> {
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
    }
    GRAPH <${queryEnv.agendaLineageGraph}> {
      ?s ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    GRAPH <${queryEnv.agendaLineageGraph}> {
      ?decision ext:tracesLineageTo ?agenda .
    }
    GRAPH <${queryEnv.tempGraph}> {
      ?decision a ?targetClass .
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

export const fillUp = async (queryEnv, agendaFilter = "") => {
  try {
    const start = moment().utc();
    const filter = [notConfidentialFilter, notBeperktOpenbaarFilter].join("\n");
    const filterAgendasWithAccess=[
      notConfidentialFilter, notBeperktOpenbaarFilter,
      sessionPublicationDateHasPassed(),
      agendaFilter
    ].join("\n");
    let targetGraph = queryEnv.targetGraph;
    logStage(`fill overheid started at: ${start}`, targetGraph);
    await addVisibleAgendas(queryEnv, filterAgendasWithAccess);
    logStage(`overheid agendas added`, targetGraph);
    await addAllRelatedToAgenda(queryEnv, filter);
    logStage(`related to agenda added`, targetGraph);
    await addRelatedToAgendaItemAndSubcase(queryEnv, filter);
    logStage(`agenda items and subcases added`, targetGraph);
    await addAllRelatedDocuments(queryEnv, filter);
    logStage('documents added', targetGraph);
    await addRelatedFiles(queryEnv);
    logStage('related files added', targetGraph);
    await fillOutDetailsOnVisibleItems(queryEnv);
    logStage('details added', targetGraph);

    // TODO this will remove everything except the changeset if we have any
    // should fix... will use lineage graph to remove things that should use targeted agenda only but that are no longer in the result set
    // always remove links to lineage agenda if not in temp graph but has link to lineage
    await removeInfoNotInTemp(queryEnv);
    logStage('removed not in temp', targetGraph);
    await cleanup(queryEnv);
    logStage('done filling overheid', targetGraph);
    const end = moment().utc();
    logStage(`fill overheid ended at: ${end}, took: ${end.diff(start, 'ms')}ms`);
  } catch (e) {
    logStage(e, queryEnv.targetGraph);
  }
};
