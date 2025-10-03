import { uuid } from 'mu';
import { querySudo, updateSudo } from './auth-sudo';
import { queryTriplestore, updateTriplestore } from './triplestore';
import { runStage, forLoopProgressBar } from './timing';
import { countTriples, deleteResource } from './query-helpers';
import { cleanupPublicationFlowDetails } from './collectors/publication-collection';
import { cleanupEmptyAgendaitemTreatments } from './collectors/decision-collection';
import { USE_DIRECT_QUERIES, MU_AUTH_PAGE_SIZE, VIRTUOSO_RESOURCE_PAGE_SIZE, KEEP_TEMP_GRAPH } from '../config';

class Distributor {
  constructor({ sourceGraph, targetGraph, model }) {
    this.sourceGraph = sourceGraph;
    this.targetGraph = targetGraph;
    this.tempGraph = `http://mu.semte.ch/graphs/temp/${uuid()}`;
    this.tempGraphSubjectsIn = `${this.tempGraph}/subjects-in`;
    this.tempGraphSubjectsOut = `${this.tempGraph}/subjects-out`;
    this.model = model;

    this.releaseOptions = {
      validateDecisionsRelease: false,
      validateDocumentsRelease: false
    };
  }

  async perform(options = { agendaUris: [], isInitialDistribution: false }) {
    if (this.collect) {
      console.log(`${this.constructor.name} started at ${new Date().toISOString()}`);

      await runStage(`Register temp graph <${this.tempGraph}>`, async () => {
        await this.registerTempGraph();
      });

      // resource collection logic implemented by subclass
      const hasCollectedResources = await this.collect(options);

      if (hasCollectedResources) {
        await runStage('Collect resource details', async () => {
          await this.collectResourceDetails();
        }, this.constructor.name);

        await runStage('Cleanup resource details', async () => {
          // Cleanup of the collected resource details of the previous step.
          // Doing cleanup as a post-processing step will be cheaper than making
          // the collectResourceDetails queries more complex with FILTER statements
          await this.cleanupResourceDetails();
        }, this.constructor.name);

        if (!options.isInitialDistribution) {
          await runStage('Cleanup previously published data', async () => {
            await this.cleanupPreviouslyPublishedData();
          }, this.constructor.name);
        }

        const count = await countTriples({ graph: this.tempGraph });
        console.log(`Temp graph <${this.tempGraph}> now contains ${count} triples.`);
        await runStage(`Copy temp graph to <${this.targetGraph}>`, async () => {
          await this.copyTempGraph();
        });
      } else {
        console.log('No resources collected in temp graph');
     }

      if (KEEP_TEMP_GRAPH) {
        console.log(`Service configured not to cleanup temp graph. Graph <${this.tempGraph}> will remain in triplestore.`);
      } else {
        await runStage(`Delete temp graph <${this.tempGraph}>`, async () => {
          await updateTriplestore(`DROP SILENT GRAPH <${this.tempGraph}>`);
          await updateTriplestore(`DROP SILENT GRAPH <${this.tempGraphSubjectsIn}>`);
          await updateTriplestore(`DROP SILENT GRAPH <${this.tempGraphSubjectsOut}>`);
        });
      }

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
          <${this.tempGraphSubjectsIn}> a ext:TempGraph .
          <${this.tempGraphSubjectsOut}> a ext:TempGraph .
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
   * We use additional temporary graphs to keep track of the subjects that have already been copied.
   * This way we avoid to paginate (combining ORDER BY with LIMIT/OFFSET) over all subjects in the temp graph,
   * which is a costly operation in Virtuoso for large datasets.
  */
  async collectResourceDetails() {
    const summary = await queryTriplestore(`
      SELECT (COUNT(?s) AS ?count) ?type WHERE {
        GRAPH <${this.tempGraph}> {
          ?s a ?type .
        }
      } GROUP BY ?type ORDER BY ?type`);

    console.log(`Temp graph <${this.tempGraph}> summary`);
    const typesWithCount = {};
    summary.results.bindings.forEach(binding => {
      const type = binding['type'].value;
      const count = parseInt(binding['count'].value);
      console.log(`\t[${count}] ${type}`);
      typesWithCount[type] = count;
    });

    console.log(`Copying temp graph to keep track of handled resources during details collection`);
    await updateTriplestore(`COPY SILENT GRAPH <${this.tempGraph}> TO <${this.tempGraphSubjectsIn}>`);
    await updateTriplestore(`COPY SILENT GRAPH <${this.tempGraph}> TO <${this.tempGraphSubjectsOut}>`);

    const types = Object.keys(typesWithCount);
    const relevantTypes = types.filter((type) => this.model.isRelevantType(type));
    const missingTypes = types.filter((type) => !this.model.isConfiguredType(type));
    if (missingTypes.length) {
      console.log(`The following types are found in the temp graph but not configured in the Yggdrasil model to be distributed or ignored. You may want to add these to the config.`);
      missingTypes.forEach((type) => console.log(`\t - ${type}`));
    }

    for (let type of relevantTypes) {
      const count = typesWithCount[type];
      const limit = VIRTUOSO_RESOURCE_PAGE_SIZE;
      const totalBatches = Math.ceil(count / limit);
      let currentBatch = 0;
      while (currentBatch < totalBatches) {
        await runStage(`Collect details of <${type}> (batch ${currentBatch + 1}/${totalBatches})`, async () => {
          // Outgoing triples
          await updateTriplestore(`
            DELETE {
              GRAPH <${this.tempGraphSubjectsIn}> {
                ?resource a <${type}> .
              }
            } INSERT {
              GRAPH <${this.tempGraph}> {
                ?resource a <${type}> .
                ?resource ?p ?o .
              }
            } WHERE {
              {
                SELECT ?resource WHERE {
                  GRAPH <${this.tempGraphSubjectsIn}> {
                    ?resource a <${type}> .
                  }
                } ORDER BY ?resource LIMIT ${VIRTUOSO_RESOURCE_PAGE_SIZE}
              }
              GRAPH <${this.sourceGraph}> {
                ?resource a <${type}> . # for Virtuoso performance
                ?resource ?p ?o .
              }
            }`);

          // Incoming triples
          await updateTriplestore(`
            DELETE {
              GRAPH <${this.tempGraphSubjectsOut}> {
                ?resource a <${type}> .
              }
            } INSERT {
              GRAPH <${this.tempGraph}> {
                ?resource a <${type}> .
                ?s ?p ?resource .
              }
            } WHERE {
              {
                SELECT ?resource WHERE {
                  GRAPH <${this.tempGraphSubjectsOut}> {
                    ?resource a <${type}> .
                  }
                } ORDER BY ?resource LIMIT ${VIRTUOSO_RESOURCE_PAGE_SIZE}
              }
              GRAPH <${this.sourceGraph}> {
                ?resource a <${type}> . # for Virtuoso performance
                ?s ?p ?resource .
              }
            }`);
        });

        currentBatch++;
      }
    }
  }

  async cleanupResourceDetails() {
    await runStage('Cleanup publication flow details that must not be published' , async () => {
      await cleanupPublicationFlowDetails(this);
    });

    await runStage('Filter empty agendaitem treatments to work around cache issue', async () => {
      await cleanupEmptyAgendaitemTreatments(this);
    });
  }

  /*
   * Step 1: Find all resources that have been published before with lineage to an agenda
   * that is in scope of this distribution process, but don't have any lineage in
   * the current tempGraph. This means the resource should no longer be visible.
   * Otherwise it would have been included in the tempGraph.
   *
   * Step 2: Find all resources that have been published before with lineage to an agenda
   * that is in scope of this distribution process, but have at least 1 property, that is not a lineage,
   * that is not in the temp graph anymore. This means the published property is stale
   * and must be removed. Otherwise it would have been included in the tempGraph.
   *
   * Step 3: Find all resources that have been published before with lineage to an agenda
   * that is in scope of this distribution process, but don't have lineage to that same
   * agenda in the current tempGraph. This means the resource should still be visible,
   * since it's included in the tempGraph, but it's published lineage must be updated.
  */
  async cleanupPreviouslyPublishedData() {
    // Step 1
    const cleanupResourcesQuery = `
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
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
    console.log(`Cleanup ${resources.length} published resources that should no longer be visible`);
    await forLoopProgressBar(resources, async (resource) => {
      await deleteResource(resource, this.targetGraph);
      await deleteResource(resource, this.targetGraph, { inverse: true });
    });

    // Step 2
    const cleanupStalePropertiesQuery = `
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      SELECT DISTINCT ?published
      WHERE {
        GRAPH <${this.tempGraph}> {
          ?agenda a besluitvorming:Agenda .
        }
        {
          {
            GRAPH <${this.targetGraph}> {
              ?published ext:tracesLineageTo ?agenda ;
                 ?p ?o .
              FILTER(?p != ext:tracesLineageTo)
            }
            FILTER NOT EXISTS {
              GRAPH <${this.tempGraph}> {
                ?published ?p ?o .
              }
            }
          }
          UNION
          {
            GRAPH <${this.targetGraph}> {
              ?published ext:tracesLineageTo ?agenda .
                 ?s ?p ?published .
              FILTER(?p != ext:tracesLineageTo)
            }
            FILTER NOT EXISTS {
              GRAPH <${this.tempGraph}> {
                ?s ?p ?published .
              }
            }
          }
        }
      }
    `;
    result = await queryTriplestore(cleanupStalePropertiesQuery);
    resources = result.results.bindings.map(b => b['published'].value);
    // From all resources that have at least 1 stale property, we're going to
    // remove all properties (not only the stale ones) that have been published already.
    // The ones that are not stale and still may be published will be copied again
    // from temp graph to target graph in a next phase.
    console.log(`Cleanup ${resources.length} published resources with stale properties`);
    await forLoopProgressBar(resources, async (resource) => {
      await deleteResource(resource, this.targetGraph);
      await deleteResource(resource, this.targetGraph, { inverse: true });
    });

    // Step 3
    const cleanupLineageQuery = `
      PREFIX besluitvorming: <https://data.vlaanderen.be/ns/besluitvorming#>
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
    console.log(`Cleanup lineages of ${lineages.length} published resources that must be updated`);
    await forLoopProgressBar(lineages, async (lineage) => {
      await updateSudo(`
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        DELETE DATA {
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
   *
   * In case copy is executed via mu-authorization only new triples (triples in temp,
   * that are not yet in target graph) are copied. This strategy has following advantages:
   * - no need to be more strict (avoiding duplicates) while collecting triples in temp.
   *   Experiments have shown that collecting data in temp is rather cheap,
   *   while copying data in batches to the target graph is expensive and time-consuming.
   * - delta's are only sent for actual changes, instead of for all triples. This greatly
   *   minimizes impact on other services (mu-search, resources, ...) to update their state.
  */
  async copyTempGraph() {
    const source = this.tempGraph;
    const target = this.targetGraph;

    if (USE_DIRECT_QUERIES) {
      await runStage(`Copy triples using graph operation without delta notifications`, async () => {
        await updateTriplestore(`COPY SILENT GRAPH <${source}> TO <${target}>`);
      });
    } else {
      const queryResult = await querySudo(`
        SELECT (COUNT(*) as ?count) WHERE {
          GRAPH <${source}> { ?s ?p ?o . }
          FILTER NOT EXISTS {
            GRAPH <${target}> { ?s ?p ?o . }
          }
        }`);
      const count = parseInt(queryResult.results.bindings[0].count.value);
      console.log(`${count} triples in graph <${source}> not found in target graph <${target}>. Going to copy these triples.`);
      const limit = MU_AUTH_PAGE_SIZE;
      const totalBatches = Math.ceil(count / limit);
      console.log(`Copying ${count} triples in batches of ${MU_AUTH_PAGE_SIZE}`);
      let currentBatch = 0;
      while (currentBatch < totalBatches) {
        await runStage(`Copy triples (batch ${currentBatch + 1}/${totalBatches})`, async () => {
          // Note: no OFFSET needed in the subquery. Pagination is inherent since
          // the WHERE clause doesn't match any longer for triples that are copied in the previous batch.
          await updateSudo(`
          INSERT {
            GRAPH <${target}> {
              ?resource ?p ?o .
            }
          } WHERE {
            SELECT ?resource ?p ?o WHERE {
              GRAPH <${source}> { ?resource ?p ?o . }
              FILTER NOT EXISTS {
                GRAPH <${target}> { ?resource ?p ?o }
              }
            } LIMIT ${limit}
          }`);
        });
        currentBatch++;
      }
    }
  }
}

export default Distributor;
