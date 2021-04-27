import { uuid } from 'mu';
import { updateSudo } from './auth-sudo';
import { queryTriplestore, updateTriplestore } from './triplestore';
import { runStage, forLoopProgressBar } from './timing';
import { countResources, countTriples } from './query-helpers';
import { USE_DIRECT_QUERIES, MU_AUTH_PAGE_SIZE, VIRTUOSO_RESOURCE_PAGE_SIZE } from '../config';

class Distributor {
  constructor({ sourceGraph, targetGraph }) {
    this.sourceGraph = sourceGraph;
    this.targetGraph = targetGraph;
    this.tempGraph = `http://mu.semte.ch/temp/${uuid()}`;

    this.releaseOptions = {
      validateDecisionsRelease: false,
      validateDocumentsRelease: false,
      validateNewsitemsRelease: false
    };
  }

  async perform(options = { agendaUris: [], isInitialDistribution: false }) {
    if (this.collect) {
      console.log(`${this.constructor.name} started at ${new Date().toISOString()}`);

      await runStage('Registered temp graph', async () => {
        await this.registerTempGraph();
      });

      await this.collect(options); // resource collection logic implemented by subclass

      await runStage('Collect resource details', async () => {
        await this.collectResourceDetails();
      }, this.constructor.name);

      if (!options.isInitialDistribution) {
        await runStage('Cleanup previously published data', async () => {
          await this.cleanupPreviouslyPublishedData();
        }, this.constructor.name);
      }

      await runStage(`Copied temp graph to <${this.targetGraph}>`, async () => {
        await this.copyTempGraph();
      });

      await runStage(`Deleted temp graph <${this.tempGraph}>`, async () => {
        await updateTriplestore(`DROP SILENT GRAPH <${this.tempGraph}>`);
      });

      console.log(`${this.constructor.name} ended at ${new Date().toISOString()}`);
    } else {
      console.warn(`Distributor ${this.constructor.name} doesn't contain a function this.collect(). Nothing to perform.`);
    }
  }

  async registerTempGraph() {
    await updateTriplestore(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      INSERT DATA {
        GRAPH <${this.tempGraph}> {
          <${this.tempGraph}> a ext:TempGraph .
        }
      }`);
  }

  /*
   * Copy all incoming and outgoing triples of the collected resources in temp graph.
   * Note, the copy operation must be executed in batches using a subquery.
   * The number of triples selected in the subquery (= VIRTUOSO_RESOURCE_PAGE_SIZE * triples_for_resource
   * should not exceed the maximum number of triples returned by the database
   * (ie. ResultSetMaxRows in Virtuoso).
   *
   * Using subquery to select all distinct subjects.
   * More info: http://vos.openlinksw.com/owiki/wiki/VOS/VirtTipsAndTricksHowToHandleBandwidthLimitExceed
  */
  async collectResourceDetails() {
    const summary = await queryTriplestore(`
      SELECT (COUNT(?s) AS ?count) ?type WHERE {
        GRAPH <${this.tempGraph}> {
          ?s a ?type .
        }
      } GROUP BY ?type ORDER BY ?type`);

    console.log(`Temp graph <${this.tempGraph}> summary`);
    summary.results.bindings.forEach(binding => {
      console.log(`\t[${binding['count'].value}] ${binding['type'].value}`);
    });

    const types = summary.results.bindings.map(b => b['type'].value);

    for (let type of types) {
      const count = await countResources({ graph: this.tempGraph, type: type });

      const limit = VIRTUOSO_RESOURCE_PAGE_SIZE;
      const totalBatches = Math.ceil(count / limit);
      let currentBatch = 0;
      while (currentBatch < totalBatches) {
        await runStage(`Copy details of <${type}> (batch ${currentBatch + 1}/${totalBatches})`, async () => {
          const offset = limit * currentBatch;

          // Outgoing triples
          await updateTriplestore(`
          INSERT {
            GRAPH <${this.tempGraph}> {
              ?resource ?p ?o .
            }
          } WHERE {
            {
              SELECT ?resource ?p ?o WHERE {
                {
                  SELECT ?resource {
                    SELECT DISTINCT ?resource {
                      GRAPH <${this.tempGraph}> {
                        ?resource a <${type}> .
                      }
                    } ORDER BY ?resource
                  } LIMIT ${limit} OFFSET ${offset}
                }
                {
                  GRAPH <${this.sourceGraph}> {
                    ?resource ?p ?o .
                  }
                }
              }
            }
          }
        `);

          // Incoming triples
          await updateTriplestore(`
          INSERT {
            GRAPH <${this.tempGraph}> {
              ?s ?p ?resource .
            }
          } WHERE {
            {
              SELECT ?s ?p ?resource WHERE {
                {
                  SELECT ?resource {
                    SELECT DISTINCT ?resource {
                      GRAPH <${this.tempGraph}> {
                        ?resource a <${type}> .
                      }
                    } ORDER BY ?resource
                  } LIMIT ${limit} OFFSET ${offset}
                }
                {
                  GRAPH <${this.sourceGraph}> {
                    ?s ?p ?resource .
                  }
                }
              }
            }
          }
        `);
        });

        currentBatch++;
      }
    }
  }

  /*
   * Step 1: Find all resources that have been published before with lineage to an agenda
   * that is in scope of this distribution process, but don't have any lineage in
   * the current tempGraph. This means the resource should no longer be visible.
   * Otherwise it would have been included in the tempGraph.
   *
   * Step 2: Find all resources that have been published before with lineage to an agenda
   * that is in scope of this distribution process, but don't have lineage to that same
   * agenda in the current tempGraph. This means the resource should still be visible,
   * since it's included in the tempGraph, but it's published lineage must be updated.
  */
  async cleanupPreviouslyPublishedData() {
    // Step 1
    const cleanupResourcesQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      SELECT DISTINCT ?published
      WHERE {
        GRAPH <${this.targetGraph}> {
          ?published ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.tempGraph}> {
          ?agenda a besluitvorming:Agenda .
          FILTER NOT EXISTS {
            ?published ext:tracesLineageTo ?anyAgenda .
          }
        }
      }
    `;
    let result = await queryTriplestore(cleanupResourcesQuery);
    let resources = result.results.bindings.map(b => b['published'].value);
    await forLoopProgressBar(resources, async (resource) => {
      await updateSudo(`
        DELETE WHERE {
          GRAPH <${this.targetGraph}> {
            <${resource}> ?p ?o .
          }
        }
      `);
      await updateSudo(`
        DELETE WHERE {
          GRAPH <${this.targetGraph}> {
            ?s ?p <${resource}> .
          }
        }
      `);
    });

    // Step 2
    const cleanupLineageQuery = `
      PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      SELECT DISTINCT ?published ?agenda
      WHERE {
        GRAPH <${this.targetGraph}> {
          ?published ext:tracesLineageTo ?agenda .
        }
        GRAPH <${this.tempGraph}> {
          ?agenda a besluitvorming:Agenda .
          FILTER NOT EXISTS {
            ?published ext:tracesLineageTo ?agenda .
          }
        }
      }
    `;

    result = await queryTriplestore(cleanupLineageQuery);
    const lineages = result.results.bindings.map(b => {
      return {
        resource: b['published'].value,
        agenda: b['agenda'].value
      };
    });
    await forLoopProgressBar(lineages, async (lineage) => {
      await updateSudo(`
        DELETE WHERE {
          GRAPH <${this.targetGraph}> {
            <${lineage.resource}> ext:tracesLineageTo <${lineage.agenda}> .
          }
        }
      `);
    });
  }

  /*
   * Copy all triples from the temp graph to the target graph
   *
   * Depending on the configuration of USE_DIRECT_QUERIES the copy queries
   * are executed via mu-authorization (resulting in delta notifications being sent)
   * or directly on Virtuoso (without delta notifications)
  */
  async copyTempGraph() {
    const source = this.tempGraph;
    const target = this.targetGraph;

    if (USE_DIRECT_QUERIES) {
      await runStage(`Copy triples using graph operation without delta notifications`, async () => {
        await updateTriplestore(`COPY SILENT GRAPH <${source}> TO <${target}>`);
      });
    } else {
      const count = await countTriples({ graph: source });
      const limit = MU_AUTH_PAGE_SIZE;
      const totalBatches = Math.ceil(count / limit);
      let currentBatch = 0;
      while (currentBatch < totalBatches) {
        await runStage(`Copy triples (batch ${currentBatch + 1}/${totalBatches})`, async () => {
          const offset = limit * currentBatch;
          await updateSudo(`
          INSERT {
            GRAPH <${target}> {
              ?resource ?p ?o .
            }
          } WHERE {
            SELECT ?resource ?p ?o WHERE {
              GRAPH <${source}> {
                ?resource ?p ?o .
              }
            } LIMIT ${limit} OFFSET ${offset}
          }
        `);
        });
        currentBatch++;
      }
    }
  }
}

export default Distributor;
