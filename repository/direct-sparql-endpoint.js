import httpContext from 'express-http-context';
import SC2 from 'sparql-client-2';
const { SparqlClient } = SC2;

const request = require('request-promise');


//==-- logic --==//

// builds a new sparqlClient
function newSparqlClient(args) {

  let options = {
    requestDefaults: { headers: { 'Connection': 'keep-alive'} }
  };

  if (httpContext.get('request')) {
    options.requestDefaults.headers['mu-session-id'] = httpContext.get('request').get('mu-session-id');
    options.requestDefaults.headers['mu-call-id'] = httpContext.get('request').get('mu-call-id');
    options.requestDefaults.headers['mu-auth-allowed-groups'] = httpContext.get('request').get('mu-auth-allowed-groups'); // groups of incoming request
  }

  if (httpContext.get('response')) {
    const allowedGroups = httpContext.get('response').get('mu-auth-allowed-groups'); // groups returned by a previous SPARQL query
    if (allowedGroups)
      options.requestDefaults.headers['mu-auth-allowed-groups'] = allowedGroups;
  }

  if(args.sudo){
    options.requestDefaults.headers['mu-auth-sudo'] = 'true';
  }
  if(process.env.VERBOSE){
    console.log(`Headers set on SPARQL client: ${JSON.stringify(options)}`);
  }

  return new SparqlClient(args.url || process.env.MU_SPARQL_ENDPOINT, options).register({
    mu: 'http://mu.semte.ch/vocabularies/',
    muCore: 'http://mu.semte.ch/vocabularies/core/',
    muExt: 'http://mu.semte.ch/vocabularies/ext/'
  });
}



// executes a query (you can use the template syntax)
function query( args, queryString, retries ) {
  if(process.env.VERBOSE){
    console.log(queryString);
  }
  if (!retries){
    retries = 0;
  }

  var options = { method: 'POST',
    url: args.url || process.env.MU_SPARQL_ENDPOINT,
    headers:
      { 'cache-control': 'no-cache',
        host: 'database:8890',
        'mu-auth-sudo': 'true',
        'connection': 'keep-alive',
        'Accept': '*'
      },
    form: {
      'content-type': 'application/sparql-results+json',
      'format': 'application/sparql-results+json'
    } };

    if(queryString.indexOf('INSERT') >= 0 || queryString.indexOf('DELETE') >=0 || queryString.indexOf('DROP')>= 0 || queryString.indexOf('CREATE') >= 0){
      options.form.update = queryString;
    }else{
      options.form.query = queryString;
    }



  if (httpContext.get('request')) {
    options.headers['mu-session-id'] = httpContext.get('request').get('mu-session-id');
    options.headers['mu-call-id'] = httpContext.get('request').get('mu-call-id');
    options.headers['mu-auth-allowed-groups'] = httpContext.get('request').get('mu-auth-allowed-groups'); // groups of incoming request
  }

  if (httpContext.get('response')) {
    const allowedGroups = httpContext.get('response').get('mu-auth-allowed-groups'); // groups returned by a previous SPARQL query
    if (allowedGroups)
      options.headers['mu-auth-allowed-groups'] = allowedGroups;
  }

  if(process.env.VERBOSE){
    console.log(`OPTIONS set on SPARQL request: ${JSON.stringify(options)}`);
  }

  return request(options).catch((e) => {
    if (retries < 5){
      console.log(`Failed executing query ${queryString}`);
      return query(args, queryString, retries + 1);
    }
    console.log(`Error: failed executing query in final try: ${queryString}
    
    ${e.message}
    
    ${e.stack}`);

    throw e;
  });
};

module.exports = {
  query
};