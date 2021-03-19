import httpContext from 'express-http-context';
import SC2 from 'sparql-client-2';
const { SparqlClient } = SC2;
const retryTimeout = parseInt(process.env.RETRY_TIMEOUT || "1000");

const request = require('request-promise');


//==-- logic --==//

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

    if (args.overrideFormHeaders){
      options.form = args.overrideFormHeaders;
    }

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

  return request(options)
  .catch((e) => {
    if (retries < 5){
      console.log(`Failed executing query ${queryString}`);
      const newRetryCount = retries + 1;
      return new Promise((resolve, reject) => {
        setTimeout(() => {
          query(args, queryString, newRetryCount)
          .then((result)=> {
            resolve(result);
          })
          .catch((error) => {
            reject(error);
          });
        }, 1 + ((newRetryCount - 1) * retryTimeout ));
      });

    }
    console.log(`Error: failed executing query in final try: ${queryString}

    ${e.message}

    ${e.stack}`);

    throw e;
  });
}

module.exports = {
  query
};
