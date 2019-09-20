import mu from 'mu';
const app = mu.app;
const bodyParser = require('body-parser');
const cors = require('cors');
import {handleDelta} from './handle-deltas';
import {directQuery, configurableQuery} from './repository/helpers';

const fillInterneOverheid = require('./repository/fill-intern-overheid');
const fillInterneRegering = require('./repository/fill-intern-regering');
const fillPublic = require('./repository/fill-public');

app.use(bodyParser.json({ type: 'application/json' , limit: '50mb' }));
app.use(cors());

const adminGraph = `http://mu.semte.ch/graphs/organizations/kanselarij`;

const builders = {
  'public': {
    env: {
      adminGraph: adminGraph,
      targetGraph:`http://mu.semte.ch/graphs/public`,
      fullRebuild: false,
      run: configurableQuery
    },
    builder: fillPublic
  },
  'intern-overheid': {
    env: {
      adminGraph: adminGraph,
      targetGraph:`http://mu.semte.ch/graphs/organizations/intern-overheid`,
      fullRebuild: false,
      run: configurableQuery
    },
    builder: fillInterneOverheid
  },
  'intern-regering': {
    env: {
      adminGraph: adminGraph,
      targetGraph: `http://mu.semte.ch/graphs/organizations/intern-regering`,
      fullRebuild: false,
      run: configurableQuery
    },
    builder: fillInterneRegering
  },
  // uses intern-regering builder with other graph and filter
  'minister': {
    env: {
      adminGraph: adminGraph,
      targetGraph: `http://mu.semte.ch/graphs/organizations/ministers`,
      fullRebuild: false,
      extraFilter: ' ',
      run: configurableQuery
    },
    builder: fillInterneRegering
  }
};

const initialLoad = function(){
  let toFillUp = '';
  if(process.env.RELOAD_ALL_DATA_ON_INIT == "true") {
    toFillUp = 'public,intern-overheid,intern-regering,minister';
  }else if (process.env.RELOAD_ALL_DATA_ON_INIT) {
    toFillUp = process.env.RELOAD_ALL_DATA_ON_INIT;
  } 

  toFillUp = toFillUp.split(",");
  const fillOptions = {};
  Object.keys(builders).map((key) => {
    const env = Object.assign({}, builders[key].env);
    env.fullRebuild = true;
    env.run = directQuery;
    fillOptions[key] = {
      env: env,
      builder: builders[key].builder
    };
  });

  const fillAll = async function(){
    while(toFillUp.length > 0){
      let target = toFillUp.pop();
      let toFill = fillOptions[target];
      if(toFill){
        await toFill.builder.fillUp(toFill.env);
      }
    }
  };
  fillAll();
};

initialLoad();

const deltaBuilders = Object.assign({}, builders);
delete deltaBuilders.public;

app.post('/delta', (req, res) => {
  return handleDelta(req, res, deltaBuilders, directQuery);
});

