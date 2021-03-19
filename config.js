const ADMIN_GRAPH = 'http://mu.semte.ch/graphs/organizations/kanselarij';
const DIRECT_SPARQL_ENDPOINT = process.env.DIRECT_SPARQL_ENDPOINT || 'http://triplestore:8890/sparql';
const LOG_DIRECT_QUERIES = [true, 'true', 1, '1', 'yes', 'Y', 'on'].includes(process.env.LOG_DIRECT_QUERIES);
const NB_OF_QUERY_RETRIES = 6;
const RETRY_TIMEOUT_MS = parseInt(process.env.RETRY_TIMEOUT || '1000');

let RELOAD_GRAPHS_ON_INIT = [];
if ([true, 'true', 1, '1', 'yes', 'Y', 'on'].includes(process.env.RELOAD_GRAPHS_ON_INIT)) {
  RELOAD_GRAPHS_ON_INIT = ['public', 'intern-overheid', 'intern-regering', 'minister'];
} else {
  RELOAD_GRAPHS_ON_INIT = (process.env.RELOAD_GRAPHS_ON_INIT || '').split(',').filter(g => g);
}

export {
  ADMIN_GRAPH,
  DIRECT_SPARQL_ENDPOINT,
  LOG_DIRECT_QUERIES,
  NB_OF_QUERY_RETRIES,
  RETRY_TIMEOUT_MS,
  RELOAD_GRAPHS_ON_INIT
}
