import mu, { app } from 'mu';
import bodyParser from 'body-parser';

import {handleDelta} from './handle-deltas';
import {cleanup, directQuery, configurableQuery} from './repository/helpers';

import fillInterneOverheid from './repository/fill-intern-overheid';
import fillInterneRegering from './repository/fill-intern-regering';
import fillKanselarij from './repository/fill-kanselarij';
import fillPublic from './repository/fill-public';

if (!process.env.DIRECT_ENDPOINT) {
    throw new Error("DIRECT_ENDPOINT not set!");
}

app.use(bodyParser.json({type: 'application/json', limit: '50mb'}));

const adminGraph = `http://mu.semte.ch/graphs/organizations/kanselarij`;

const builders = {
    'public': {
        env: {
            adminGraph: adminGraph,
            targetGraph: `http://mu.semte.ch/graphs/public`,
            fullRebuild: false,
            run: configurableQuery
        },
        builder: fillPublic
    },
    'intern-overheid': {
        env: {
            adminGraph: adminGraph,
            targetGraph: `http://mu.semte.ch/graphs/organizations/intern-overheid`,
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
    'kanselarij': {
        skipInitialLoad: true,
        env: {
            adminGraph: adminGraph,
            targetGraph: `http://mu.semte.ch/graphs/organizations/kanselarij-mirror`,
            fullRebuild: false,
            run: configurableQuery
        },
        builder: fillKanselarij
    },
    // uses intern-regering builder with other graph and filter
    'minister': {
        env: {
            adminGraph: adminGraph,
            targetGraph: `http://mu.semte.ch/graphs/organizations/minister`,
            fullRebuild: false,
            extraFilter: ' ',
            run: configurableQuery
        },
        builder: fillInterneRegering
    }
};

async function initialLoad() {
    let toFillUp = '';
    if (process.env.RELOAD_ALL_DATA_ON_INIT == "true") {
        toFillUp = 'public,intern-overheid,intern-regering,minister';
    } else if (process.env.RELOAD_ALL_DATA_ON_INIT) {
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

    const fillAll = async function () {
        while (toFillUp.length > 0) {
            let target = toFillUp.pop();
            let toFill = fillOptions[target];
            if (toFill) {
                await toFill.builder.fillUp(toFill.env);
            }
        }
    };
    await fillAll();
};

async function startup() {
    await cleanup();
    await initialLoad();
}

startup();

const deltaBuilders = Object.assign({}, builders);
delete deltaBuilders.kanselarij;
delete deltaBuilders.public;

app.post('/delta', (req, res) => {
    return handleDelta(req, res, deltaBuilders, directQuery);
});

if(process.env.ALLOW_DOWNLOADS === "true"){
    const downloadRequests = {};
    app.get('/downloadZittingResult', async (req, res) => {
        const downloadId = req.query.id;
        const downloadRequest = downloadRequests[downloadId];
        if(!downloadRequest){
          res.status(404).send({ status: "not found" } );
          return;
        }
        let done = downloadRequest.status != "loading";
        if(done){
            if(downloadRequest.status == "error"){
              res.status(500).send(downloadRequest.result);
            }
            res.status(200).send(downloadRequest.result);
            delete downloadRequests[downloadId];
        }else{
            res.send(downloadRequest.status);
        }
    });

    app.get('/downloadZitting', async (req, res) => {
        let queryString = `
prefix mu: <http://mu.semte.ch/vocabularies/core/>
prefix besluit: <http://data.vlaanderen.be/ns/besluit#>

PREFIX  besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>

select distinct(?agenda) where {
   
  ?agenda besluitvorming:isAgendaVoor ?zitting.
 ?zitting mu:uuid "${req.query.zitting}"
 
}`;
        const queryResult = await directQuery(queryString);
        const json = JSON.parse(queryResult);

        const agendas = json.results.bindings.map((binding) => {
            console.log('binding');
            console.log(binding);
            return binding.agenda.value;
        });
        res.setHeader('Content-disposition', 'attachment; filename=zitting.ttl' );
        const downloadId = mu.uuid();
        downloadRequests[downloadId] = {status: "loading"};
        res.send(downloadId);
        try{
            const result = await builders["kanselarij"].builder.fillUp(builders["kanselarij"].env, agendas, {
                toFile: true,
                anonymize: req.query.anonymize !== "false"
            });
            downloadRequests[downloadId] = {
              status: "done",
              result: result
            }
        }catch(e){
          downloadRequests[downloadId] = {
            status: "error",
            result: "" + e
          }
        }
    });
}
