import { queryTriplestore, updateTriplestore } from './triplestore';
import { RELOAD_ON_INIT } from '../config';
import { reduceChangesets, fetchRelatedAgendas } from './delta-handling';
import  {
  MinisterDistributor,
  CabinetDistributor,
  GovernmentDistributor,
  PublicDistributor
} from './distributors';

export default class Yggdrasil {

  constructor() {
    this.isProcessing = false;
    this.distributors = {
      'minister': new MinisterDistributor(),
      'intern-regering': new CabinetDistributor(),
      'intern-overheid': new GovernmentDistributor(),
      'public': new PublicDistributor()
    };
  }

  get deltaDistributors() {
    return [
      this.distributors['minister'],
      this.distributors['intern-regering'],
      this.distributors['intern-overheid']
    ];
  }

  async initialize() {
    await this.cleanupTempGraphs();
    await this.initialLoad();
  }

  async initialLoad() {
    if (RELOAD_ON_INIT.length) {
      try {
        this.isProcessing = true;
        console.log(`Distributors ${RELOAD_ON_INIT.join(',')} are configured to be reloaded on initialization. Make sure the target graphs are cleared manually.`);
        for (let key of RELOAD_ON_INIT) {
          const distributor = this.distributors[key];
          if (distributor) {
            await distributor.perform({ isInitialDistribution: true });
          } else {
            console.log(`No distributor found for key '${key}'. Skipping initial load of this key.`);
          }
        }
      } catch (e) {
        console.log('Someting went wrong while initializing Yggdrasil');
        console.log(e);
      } finally {
        this.isProcessing = false;
      }
    } else {
      console.log(`No distributors configured to be reloaded on initialization.`);
    }
  };

  async processDeltas(cache) {
    if (!cache.isEmpty) {
      if (this.isProcessing) {
        console.log('Yggdrasil process already running. Not triggering new delta handling now.');
      } else {
        try {
          this.isProcessing = true;
          const delta = cache.clear();
          const subjects = reduceChangesets(delta);
          const agendas = await fetchRelatedAgendas(subjects);
          if (agendas.length) {
            for (let distributor of this.deltaDistributors) {
              await distributor.perform({ agendaUris: agendas });
            }
          } else {
            console.log('Deltas not related to any agenda. Nothing to distribute');
          }
        } catch(e) {
          console.log("Someting went wrong while processing delta's");
          console.log(e);
        } finally {
          this.isProcessing = false;
        }
      }
    }
  }

  async cleanupTempGraphs() {
    const result = await queryTriplestore(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT ?g
    WHERE {
      GRAPH ?g {
        ?g a ext:TempGraph
      }
    }
  `);

    if (result.results && result.results.bindings) {
      console.log(`Found ${result.results.bindings.length} old temporary graphs. These graphs will be removed before going further.`);
      for (let binding of result.results.bindings) {
        console.log(`Dropping graph ${binding['g'].value}`);
        await updateTriplestore(`DROP SILENT GRAPH <${binding.g.value}>`);
      }
    }
  };
}
