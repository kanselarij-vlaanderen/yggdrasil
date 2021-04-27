import Distributor from '../distributor';
import { runStage } from '../timing';
import { updateTriplestore } from '../triplestore';
import { ADMIN_GRAPH, PUBLIC_GRAPH } from '../../constants';

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
  }

  /*
   * Collect all instances of classes marked as public
  */
  async collectPublicResources() {
    const publicResourceQuery = `
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
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
