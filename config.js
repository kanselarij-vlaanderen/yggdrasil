const ADMIN_GRAPH = 'http://mu.semte.ch/graphs/organizations/kanselarij';
const MINISTER_GRAPH = 'http://mu.semte.ch/graphs/organizations/minister';
const CABINET_GRAPH = 'http://mu.semte.ch/graphs/organizations/intern-regering';
const GOVERNMENT_GRAPH = 'http://mu.semte.ch/graphs/organizations/intern-overheid';
const PUBLIC_GRAPH = 'http://mu.semte.ch/graphs/public';

const DIRECT_SPARQL_ENDPOINT = process.env.DIRECT_SPARQL_ENDPOINT || 'http://triplestore:8890/sparql';
const LOG_DIRECT_QUERIES = [true, 'true', 1, '1', 'yes', 'Y', 'on'].includes(process.env.LOG_DIRECT_QUERIES);
const USE_DIRECT_QUERIES = [true, 'true', 1, '1', 'yes', 'Y', 'on'].includes(process.env.USE_DIRECT_QUERIES);
const NB_OF_QUERY_RETRIES = 2; // TODO update to 6
const RETRY_TIMEOUT_MS = parseInt(process.env.RETRY_TIMEOUT || '1000');
const LOG_SPARQL_ALL = [true, 'true', 1, '1', 'yes', 'Y', 'on'].includes(process.env.LOG_SPARQL_ALL);

let RELOAD_GRAPHS_ON_INIT = [];
if ([true, 'true', 1, '1', 'yes', 'Y', 'on'].includes(process.env.RELOAD_GRAPHS_ON_INIT)) {
  RELOAD_GRAPHS_ON_INIT = ['public', 'intern-overheid', 'intern-regering', 'minister'];
} else {
  RELOAD_GRAPHS_ON_INIT = (process.env.RELOAD_GRAPHS_ON_INIT || '').split(',').filter(g => g);
}

const MU_AUTH_PAGE_SIZE = parseInt(process.env.MU_AUTH_PAGE_SIZE || '2500');
const VIRTUOSO_RESOURCE_PAGE_SIZE = parseInt(process.env.VIRTUOSO_RESOURCE_PAGE_SIZE || '25000');
const VALUES_BLOCK_SIZE = parseInt(process.env.VALUES_BLOCK_SIZE || '100');

const DESIGN_AGENDA_STATUS = 'http://kanselarij.vo.data.gift/id/agendastatus/2735d084-63d1-499f-86f4-9b69eb33727f';
const AGENDAITEM_FORMALLY_OK_STATUS = 'http://kanselarij.vo.data.gift/id/concept/goedkeurings-statussen/CC12A7DB-A73A-4589-9D53-F3C2F4A40636';

const ACCESS_LEVEL_CABINET = 'http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/d335f7e3-aefd-4f93-81a2-1629c2edafa3'; // intern regering
const ACCESS_LEVEL_GOVERNMENT = 'http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/abe4c18d-13a9-45f0-8cdd-c493eabbbe29'; // intern overheid
const ACCESS_LEVEL_PUBLIC = 'http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/6ca49d86-d40f-46c9-bde3-a322aa7e5c8e';

const DECISION_STATUS_APPROVED = 'http://kanselarij.vo.data.gift/id/concept/beslissings-resultaat-codes/56312c4b-9d2a-4735-b0b1-2ff14bb524fd';

export {
  ADMIN_GRAPH,
  MINISTER_GRAPH,
  CABINET_GRAPH,
  GOVERNMENT_GRAPH,
  PUBLIC_GRAPH,
  DIRECT_SPARQL_ENDPOINT,
  LOG_DIRECT_QUERIES,
  USE_DIRECT_QUERIES,
  NB_OF_QUERY_RETRIES,
  RETRY_TIMEOUT_MS,
  RELOAD_GRAPHS_ON_INIT,
  MU_AUTH_PAGE_SIZE,
  VIRTUOSO_RESOURCE_PAGE_SIZE,
  VALUES_BLOCK_SIZE,
  DESIGN_AGENDA_STATUS,
  AGENDAITEM_FORMALLY_OK_STATUS,
  ACCESS_LEVEL_CABINET,
  ACCESS_LEVEL_GOVERNMENT,
  ACCESS_LEVEL_PUBLIC,
  DECISION_STATUS_APPROVED
}
