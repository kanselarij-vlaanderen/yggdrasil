import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import moment from 'moment';
mu.query = querySudo;

import { removeInfoNotInTemp, notConfidentialFilter, addRelatedFiles,
  cleanup, fillOutDetailsOnVisibleItems, addAllRelatedToAgenda, addRelatedToAgendaItemAndSubcase,
  notBeperktOpenbaarFilter, notInternOverheidFilter, logStage, runStage,
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
      
      ${extraFilters}
    }
  }`;
  return queryEnv.run(query);
};

const addVisibleDecisions = (queryEnv, extraFilters) => {
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a besluit:Besluit.
      ?s ext:tracesLineageTo ?agenda.
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?agendaitem a besluit:Agendapunt.
      ?agendaitem ext:tracesLineageTo ?agenda.
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?subcase besluitvorming:isGeagendeerdVia ?agendaitem.
      ?subcase ext:procedurestapHeeftBesluit ?s.
      ?s besluitvorming:goedgekeurd "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
      
      ${extraFilters}
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
      ?target a ?targetClass .
      ?target ext:tracesLineageTo ?agenda .
    }
    GRAPH <${queryEnv.adminGraph}> {
      { {
          ?target a besluit:Besluit .
        } UNION {
          ?target a besluit:Agendapunt.
          FILTER NOT EXISTS {
              ?subcase besluitvorming:isGeagendeerdVia ?target.
          }
        }
      }
      ?s a ?thing.
       { { ?target ?p ?s . } 
        UNION
        { ?target ?p ?version .
          ?s <http://data.vlaanderen.be/ns/besluitvorming#heeftVersie> ?version .
        }
      }
      
      VALUES ( ?thing ) {
        ( foaf:Document )
        ( ext:DocumentVersie )
      }

      ${extraFilters}

    }
  }`;
  return queryEnv.run(query);
};

const notADecisionFilter = `
 FILTER ( ?thing != besluit:Besluit )
`;

export const fillUp = async (queryEnv, agendas) => {
  try {
    const start = moment().utc();
    const filter = [notConfidentialFilter, notBeperktOpenbaarFilter].join("\n");
    const agendaFilter = filterAgendaMustBeInSet(agendas);
    const filterAgendasWithAccess=[
      notConfidentialFilter, notBeperktOpenbaarFilter,
      // TODO activate when implemented in frontend sessionPublicationDateHasPassed(),
      agendaFilter
    ].join("\n");
    let targetGraph = queryEnv.targetGraph;
    logStage(start, `fill overheid started at: ${start}`, targetGraph);
    await runStage(`overheid agendas added`, queryEnv, () => {
      return addVisibleAgendas(queryEnv, filterAgendasWithAccess);
    });
    await runStage('related to agenda added', queryEnv, () => {
      return addAllRelatedToAgenda(queryEnv, notConfidentialFilter);
    });
    await runStage('related to agendaitem and subcase added', queryEnv, () => {
      return addRelatedToAgendaItemAndSubcase(queryEnv, [notConfidentialFilter, notADecisionFilter].join("\n"));
    });
    await runStage('visible decisions added', queryEnv, () => {
      return addVisibleDecisions(queryEnv, notConfidentialFilter);
    });
    await runStage('documents added', queryEnv, () => {
      return addAllRelatedDocuments(queryEnv, filter);
    });
    await runStage('related files added', queryEnv, () => {
      return addRelatedFiles(queryEnv);
    });
    await runStage('details added', queryEnv, () => {
      return fillOutDetailsOnVisibleItems(queryEnv);
    });
    await runStage('lineage updated', queryEnv, () => {
      return removeThingsWithLineageNoLongerInTemp(queryEnv, agendas);
    });
    if(queryEnv.fullRebuild){
      await runStage('removed info not in temp', queryEnv, () => {
        return removeInfoNotInTemp(queryEnv);
      });
    }
    await runStage('done filling overheid', queryEnv, () => {
      return cleanup(queryEnv);
    });
    const end = moment().utc();
    logStage(start,`fill overheid ended at: ${end}`, targetGraph);
  } catch (e) {
    logStage(e, queryEnv.targetGraph);
  }
};
