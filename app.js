import mu from 'mu';
import {query} from './repository/direct-sparql-endpoint';
const app = mu.app;
const bodyParser = require('body-parser');
const cors = require('cors');
const cron = require('node-cron');

const fillInterneOverheid = require('./repository/fill-intern-overheid');
const fillInterneRegering = require('./repository/fill-intern-regering');

app.use(bodyParser.json({ type: 'application/*+json' }));
app.use(cors());

const configurableQuery = function(queryString, direct){
  return query({
    sudo: true,
    url: direct?process.env.DIRECT_ENDPOINT:undefined
  }, queryString);
};

//overheid
const queryEnvOverheid = {
  tempGraph: `http://mu.semte.ch/temp/${mu.uuid()}`,
  adminGraph:`http://mu.semte.ch/graphs/organizations/kanselarij`,
  targetGraph:`http://mu.semte.ch/graphs/organizations/users`,
  run: configurableQuery
};


// regering
const queryEnvRegering = {
  tempGraph: `http://mu.semte.ch/temp/${mu.uuid()}`,
  adminGraph: `http://mu.semte.ch/graphs/organizations/kanselarij`,
  targetGraph: `http://mu.semte.ch/graphs/organizations/kabinetten`,
  run: configurableQuery
};


if(process.env.RELOAD_ALL_DATA_ON_INIT){

  let queryEnvOverheidSetup = Object.assign({}, queryEnvOverheid);
  let queryEnvRegeringSetup = Object.assign({}, queryEnvRegering);
  queryEnvOverheidSetup.run = function(queryString){
    return configurableQuery(queryString, true);
  };
  queryEnvRegeringSetup.run = function(queryString){
    return configurableQuery(queryString, true);
  };



  fillInterneOverheid.fillUp(queryEnvOverheidSetup);
  fillInterneRegering.fillUp(queryEnvRegeringSetup);
}
