import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import moment from 'moment';
mu.query = querySudo;

import { removeInfoNotInTemp, notConfidentialFilter, addRelatedFiles,
  cleanup, fillOutDetailsOnVisibleItems, addAllRelatedToAgenda, addRelatedToAgendaItemAndSubcase,
  notInternRegeringFilter, notInternOverheidFilter, logStage, runStage,
  cleanupBasedOnLineage, filterAgendaMustBeInSet, generateTempGraph, copyTempToTarget
} from './helpers';

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
  return queryEnv.run(query, true);
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
      ?agenda dct:hasPart ?agendaitem.
      ?agenda besluit:isAangemaaktVoor ?session.
      ?session ext:releasedDecisions ?date.
      ?subcase besluitvorming:isGeagendeerdVia ?agendaitem.
      ?subcase ext:procedurestapHeeftBesluit ?s.
      ?s besluitvorming:goedgekeurd "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
      
      ${extraFilters}
    }
  }`;
  return queryEnv.run(query, true);
};

const addAllRelatedDocuments = async (queryEnv, extraFilters) => {
  const queryTemplate = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a $type .
      ?s ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    { SELECT ?target ?agenda WHERE {
      GRAPH <${queryEnv.tempGraph}> {
        ?target a ?targetClass .
        ?target ext:tracesLineageTo ?agenda .
      }
    } }
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
      ?agenda (besluit:isAangemaaktVoor / ext:releasedDocuments) ?date.

      $REPLACECONSTRAINT

      ${extraFilters}

    }
  }`;

  const constraints = [`
		?s a ext:DocumentVersie .
		?target ( ext:bevatDocumentversie | ext:bevatReedsBezorgdeDocumentversie | ext:bevatAgendapuntDocumentversie | ext:bevatReedsBezorgdAgendapuntDocumentversie | ext:mededelingBevatDocumentversie | ext:documentenVoorPublicatie | ext:documentenVoorBeslissing | ext:getekendeDocumentVersiesVoorNotulen | dct:hasPart | prov:generated ) / ^besluitvorming:heeftVersie  ?s .
		FILTER NOT EXISTS {
			GRAPH <${queryEnv.tempGraph}> {
				?s a ext:DocumentVersie .
			}
		}      
  `,`
    ?s a foaf:Document .
    ?target (dct:hasPart | ext:beslissingsfiche | ext:getekendeNotulen ) ?s .
    FILTER NOT EXISTS {
			GRAPH <${queryEnv.tempGraph}> {
				?s a foaf:Document .
			}
		}
  `];

  await queryEnv.run(queryTemplate.split('$REPLACECONSTRAINT').join(constraints[0]).split('$type').join('ext:DocumentVersie'), true);
  await queryEnv.run(queryTemplate.split('$REPLACECONSTRAINT').join(constraints[1]).split('$type').join('foaf:Document'), true);
};

const notADecisionFilter = `
 FILTER ( ?thing != besluit:Besluit )
`;

export const fillUp = async (queryEnv, agendas) => {

  try {
    const start = moment().utc();
    await generateTempGraph(queryEnv);
    const filter = [notConfidentialFilter, notInternRegeringFilter].join("\n");
    const agendaFilter = filterAgendaMustBeInSet(agendas);
    const filterAgendasWithAccess=[
      notConfidentialFilter, notInternRegeringFilter,
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
      return cleanupBasedOnLineage(queryEnv, agendas);
    });
    if(queryEnv.fullRebuild){
      await runStage('removed info not in temp', queryEnv, () => {
        return removeInfoNotInTemp(queryEnv);
      });
    }
    await runStage('copy temp to target', queryEnv, () => {
      return copyTempToTarget(queryEnv);
    });
    await runStage('done filling overheid', queryEnv, () => {
      return cleanup(queryEnv);
    });
    const end = moment().utc();
    logStage(start,`fill overheid ended at: ${end}`, targetGraph);
  } catch (e) {
    logStage(e, queryEnv.targetGraph);
    try {
      cleanup(queryEnv);
    }catch (e2) {
      console.log(e2);
    }
  }
};
