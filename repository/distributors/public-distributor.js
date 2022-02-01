import Distributor from '../distributor';
import { runStage } from '../timing';
import { updateTriplestore } from '../triplestore';
import { ADMIN_GRAPH, PUBLIC_GRAPH } from '../../constants';
import { countResources } from '../query-helpers';

/**
 * Distributor for public data
 */
export default class PublicDistributor extends Distributor {
  constructor() {
    super({
      sourceGraph: ADMIN_GRAPH,
      targetGraph: PUBLIC_GRAPH
    });
  }

  async collect(options) {
    await runStage('Collect public classes', async () => {
      await this.collectPublicResources();
    }, this.constructor.name);

    const nbOfResources = await countResources({
      graph: this.tempGraph,
      type: 'http://mu.semte.ch/vocabularies/ext/PublicClass'
    });

    return nbOfResources > 0;
  }

  /*
   * Collect all instances of classes marked as public
  */
  async collectPublicResources() {
    const publicResourceQuery = `
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT {
        GRAPH <${this.tempGraph}> {
          ?s a ?type .
        }
      } WHERE {
        GRAPH <${this.sourceGraph}> {
          ?type a ext:PublicClass .
          ?s a ?type .
        }
      }`;
    await updateTriplestore(publicResourceQuery);
  }
}
