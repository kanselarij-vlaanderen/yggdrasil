import { app, errorHandler } from 'mu';
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
    scheduleDeltaProcessing();
  }

  res.status(202).send();
});

function scheduleDeltaProcessing() {
  setTimeout( () => {
    hasTimeout = false;
    triggerDeltaProcessing(cache);
  }, DELTA_INTERVAL_MS );
  hasTimeout = true;
}

async function triggerDeltaProcessing(cache) {
  await yggdrasil.processDeltas(cache);
  if (!cache.isEmpty) {
    triggerDeltaProcessing(cache);
  }
}

app.use(errorHandler);
