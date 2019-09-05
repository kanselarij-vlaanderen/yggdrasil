import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
mu.query = querySudo;
import moment from 'moment';
import { removeInfoNotInTemp, addRelatedFiles, cleanup,
  fillOutDetailsOnVisibleItems, addAllRelatedDocuments,
  addAllRelatedToAgenda, addRelatedToAgendaItemAndSubcase,
  notBeperktOpenbaarFilter, notInternOverheidFilter, notConfidentialFilter,
  logStage, removeThingsWithLineageNoLongerInTemp, filterAgendaMustBeInSet
} from './helpers';

const addVisibleAgendas = (queryEnv, extraFilter) => {
  extraFilter = extraFilter || "";

  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a <http://data.vlaanderen.be/ns/besluitvorming#Agenda>.
      ?s ext:tracesLineageTo ?s.
    }
  } WHERE {
    GRAPH <${queryEnv.adminGraph}> {
      ?s a <http://data.vlaanderen.be/ns/besluitvorming#Agenda>.
      ?s ext:agendaNaam ?naam.
      FILTER(?naam != "Ontwerpagenda")
      
      ${extraFilter}
    }
  }`;
  return queryEnv.run(query);
};

export const fillUp = async (queryEnv, agendas) => {
  try{
    const start = moment().utc();
    const agendaFilter = filterAgendaMustBeInSet(agendas);
    const targetGraph = queryEnv.targetGraph;
    console.log(`fill regering started at: ${start}`);
    let stageStart = moment().utc();
    await addVisibleAgendas(queryEnv, agendaFilter);
    logStage(stageStart, 'agendas added', targetGraph);
    stageStart = moment().utc();
    await addAllRelatedToAgenda(queryEnv, notConfidentialFilter);
    logStage(stageStart, 'related to agenda added', targetGraph);
    stageStart = moment().utc();
    await addRelatedToAgendaItemAndSubcase(queryEnv, notConfidentialFilter);
    logStage(stageStart, 'agendaitem and subcase added', targetGraph);
    stageStart = moment().utc();
    await addAllRelatedDocuments(queryEnv, notConfidentialFilter);
    logStage(stageStart, 'related documents added', targetGraph);
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
    logStage(stageStart, 'cleaned up', targetGraph);
    const end = moment().utc();
    logStage(start, `fill regering ended at: ${end}`, targetGraph);
  }catch (e) {
    console.log(e);
  }
};