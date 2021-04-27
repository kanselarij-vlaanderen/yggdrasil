const DIRECT_SPARQL_ENDPOINT = process.env.DIRECT_SPARQL_ENDPOINT || 'http://triplestore:8890/sparql';
const USE_DIRECT_QUERIES = [true, 'true', 1, '1', 'yes', 'Y', 'on'].includes(process.env.USE_DIRECT_QUERIES);
const NB_OF_QUERY_RETRIES = 2; // TODO update to 6
const RETRY_TIMEOUT_MS = parseInt(process.env.RETRY_TIMEOUT || '1000');
const LOG_DIRECT_QUERIES = [true, 'true', 1, '1', 'yes', 'Y', 'on'].includes(process.env.LOG_DIRECT_QUERIES);
const LOG_SPARQL_ALL = [true, 'true', 1, '1', 'yes', 'Y', 'on'].includes(process.env.LOG_SPARQL_ALL);

let RELOAD_ON_INIT = [];
if ([true, 'true', 1, '1', 'yes', 'Y', 'on'].includes(process.env.RELOAD_ON_INIT)) {
  RELOAD_ON_INIT = ['public', 'intern-overheid', 'intern-regering', 'minister'];
} else {
  RELOAD_ON_INIT = (process.env.RELOAD_ON_INIT || '').split(',').filter(g => g).map(g => g.trim());
}

const MU_AUTH_PAGE_SIZE = parseInt(process.env.MU_AUTH_PAGE_SIZE || '2500');
const VIRTUOSO_RESOURCE_PAGE_SIZE = parseInt(process.env.VIRTUOSO_RESOURCE_PAGE_SIZE || '25000');
const VALUES_BLOCK_SIZE = parseInt(process.env.VALUES_BLOCK_SIZE || '100');

export {
  DIRECT_SPARQL_ENDPOINT,
  LOG_DIRECT_QUERIES,
  USE_DIRECT_QUERIES,
  NB_OF_QUERY_RETRIES,
  RETRY_TIMEOUT_MS,
  RELOAD_ON_INIT,
  MU_AUTH_PAGE_SIZE,
  VIRTUOSO_RESOURCE_PAGE_SIZE,
  VALUES_BLOCK_SIZE
}
