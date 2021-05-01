import mu, { app, errorHandler } from 'mu';
import bodyParser from 'body-parser';
import Yggdrasil from './repository/yggdrasil';
import DeltaCache from './repository/delta-cache';
import { LOG_INCOMING_DELTA, DELTA_INTERVAL_MS } from './config';

/* Accept application/json format from delta-notifier */
app.use(bodyParser.json({
  type: function(req) { return /^application\/json/.test(req.get('content-type')); },
  limit: '50mb'
}));


/* Initialize service */

const yggdrasil = new Yggdrasil();
yggdrasil.initialize();

const cache = new DeltaCache();
let hasTimeout = null;

/* Endpoints */

app.post('/delta', async function( req, res ) {
  const delta = req.body;

  if (LOG_INCOMING_DELTA)
    console.log(`Receiving delta ${JSON.stringify(delta)}`);

  cache.push(...delta);

  if ( !hasTimeout ) {
    triggerTimeout();
  }

  res.status(202).send();
});

function triggerTimeout(){
  setTimeout( () => {
    hasTimeout = false;
    yggdrasil.processDeltas(cache);
    triggerTimeout();
  }, DELTA_INTERVAL_MS );
  hasTimeout = true;
}

app.use(errorHandler);

// TODO add download endpoints
// if (process.env.ALLOW_DOWNLOADS === "true"){
//     const downloadRequests = {};
//     app.get('/downloadZittingResult', async (req, res) => {
//         const downloadId = req.query.id;
//         const downloadRequest = downloadRequests[downloadId];
//         if(!downloadRequest){
//           res.status(404).send({ status: "not found" } );
//           return;
//         }
//         let done = downloadRequest.status != "loading";
//         if(done){
//             if(downloadRequest.status == "error"){
//               res.status(500).send(downloadRequest.result);
//             }
//             res.status(200).send(downloadRequest.result);
//             delete downloadRequests[downloadId];
//         }else{
//             res.send(downloadRequest.status);
//         }
//     });

//     app.get('/downloadZitting', async (req, res) => {
//         let queryString = `
// prefix mu: <http://mu.semte.ch/vocabularies/core/>
// prefix besluit: <http://data.vlaanderen.be/ns/besluit#>

// PREFIX  besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>

// select distinct(?agenda) where {

//   ?agenda besluitvorming:isAgendaVoor ?zitting.
//  ?zitting mu:uuid "${req.query.zitting}"

// }`;
//         const queryResult = await directQuery(queryString);
//         const json = JSON.parse(queryResult);

//         const agendas = json.results.bindings.map((binding) => {
//             console.log('binding');
//             console.log(binding);
//             return binding.agenda.value;
//         });
//         res.setHeader('Content-disposition', 'attachment; filename=zitting.ttl' );
//         const downloadId = mu.uuid();
//         downloadRequests[downloadId] = {status: "loading"};
//         res.send(downloadId);
//         try{
//             const result = await builders["kanselarij"].builder.fillUp(builders["kanselarij"].env, agendas, {
//                 toFile: true,
//                 anonymize: req.query.anonymize !== "false"
//             });
//             downloadRequests[downloadId] = {
//               status: "done",
//               result: result
//             }
//         }catch(e){
//           downloadRequests[downloadId] = {
//             status: "error",
//             result: "" + e
//           }
//         }
//     });
// }
