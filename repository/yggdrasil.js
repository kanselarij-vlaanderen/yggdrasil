import fillInterneOverheid from './fill-intern-overheid';
import fillInterneRegering from './fill-intern-regering';
import fillKanselarij from './fill-kanselarij';
import fillPublic from './fill-public';
import { configurableQuery } from './helpers';
import { queryTriplestore, updateTriplestore } from './triplestore';
import { ADMIN_GRAPH, RELOAD_GRAPHS_ON_INIT } from '../config';

class Yggdrasil {
  builders = {
    'public': {
      env: {
        adminGraph: ADMIN_GRAPH,
        targetGraph: 'http://mu.semte.ch/graphs/public',
        fullRebuild: false,
        run: configurableQuery
      },
      builder: fillPublic
    },
    'intern-overheid': {
      env: {
        adminGraph: ADMIN_GRAPH,
        targetGraph: 'http://mu.semte.ch/graphs/organizations/intern-overheid',
        fullRebuild: false,
        run: configurableQuery
      },
      builder: fillInterneOverheid
    },
    'intern-regering': {
      env: {
        adminGraph: ADMIN_GRAPH,
        targetGraph: 'http://mu.semte.ch/graphs/organizations/intern-regering',
        fullRebuild: false,
        run: configurableQuery
      },
      builder: fillInterneRegering
    },
    'kanselarij': {
      skipInitialLoad: true,
      env: {
        adminGraph: ADMIN_GRAPH,
        targetGraph: 'http://mu.semte.ch/graphs/organizations/kanselarij-mirror',
        fullRebuild: false,
        run: configurableQuery
      },
      builder: fillKanselarij
    },
    // uses intern-regering builder with other graph and filter
    'minister': {
      env: {
        adminGraph: ADMIN_GRAPH,
        targetGraph: 'http://mu.semte.ch/graphs/organizations/minister',
        fullRebuild: false,
        extraFilter: ' ',
        run: configurableQuery
      },
      builder: fillInterneRegering
    }
  };

  async initialize() {
    await this.cleanupTempGraphs();
    await this.initialLoad();
  }

  async cleanupTempGraphs() {
    const result = await queryTripleStore(`
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

  async initialLoad() {
    const toFillUp = RELOAD_GRAPHS_ON_INIT.slice(0); // make a copy

    const buildersOnInit = {};
    Object.keys(this.builders).forEach((key) => {
      const env = Object.assign({}, this.builders[key].env);
      env.fullRebuild = true;
      env.run = queryTriplestore;
      buildersOnInit[key] = {
        env: env,
        builder: this.builders[key].builder
      };
    });

    while (toFillUp.length > 0) {
      let target = toFillUp.pop();
      let toFill = buildersOnInit[target];
      if (toFill) {
        await toFill.builder.fillUp(toFill.env);
      }
    }
  };
}

export default Yggdrasil;
