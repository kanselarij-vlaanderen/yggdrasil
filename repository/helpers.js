import mu from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
mu.query = querySudo;
import moment from 'moment';

const batchSize = process.env.BATCH_SIZE || 3000;

const parseSparQlResults = (data, multiValueProperties = []) => {
	const vars = data.head.vars;
	return data.results.bindings.map(binding => {
		let obj = {};

		vars.forEach(varKey => {
			if (binding[varKey]){
				let val = binding[varKey].value;
				if (multiValueProperties.includes(varKey)){
					val = val.split('|')
				}
				obj[varKey] = val;
			}else {
				obj[varKey] = null;
			}
		});
		return obj;
	})
};

const logStage = (start, logMessage, graph) => {
	const time = moment().utc().diff(start, 'seconds', true);
	console.log(`${graph} => ${logMessage} -- time: ${time.toFixed(3)}s`);
};

const removeInfoNotInTemp = (queryEnv) => {
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  DELETE {
    GRAPH <${queryEnv.targetGraph}> {
      ?s ?p ?o.
    }
  } WHERE {
    GRAPH <${queryEnv.targetGraph}> {
			?s ?p ?o.
			?s a ?type.

      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s ?p ?o.
        }
      }
    }
  }`;
  return queryEnv.run(query);
};

// TODO better do the inverse, but that means we should do it on items that truly have confidentiality... to be seen
const notConfidentialFilter = `
    FILTER NOT EXISTS {
      ?s ext:vertrouwelijk "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
    }
`;

const notBeperktOpenbaarFilter = `
    FILTER NOT EXISTS {
      ?s ?accessPredicate <http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/abe4c18d-13a9-45f0-8cdd-c493eabbbe29> .
      FILTER(?accessPredicate in (
        <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorProcedurestap>, 
        <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDocument>,
        <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDossier> ))
    }
`;

const notInternOverheidFilter = `
    FILTER NOT EXISTS {
      ?s ?accessPredicate <http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/d335f7e3-aefd-4f93-81a2-1629c2edafa3> .
      FILTER(?accessPredicate in (
        <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorProcedurestap>, 
        <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDocument>,
        <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDossier> ))
    }
`;

const addRelatedFiles = (queryEnv, extraFilters) => {
	extraFilters = extraFilters || '';

  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a nfo:FileDataObject .
      ?second a nfo:FileDataObject .
      ?s ext:tracesLineageTo ?agenda .
      ?second ext:tracesLineageTo ?agenda .
    }
	} WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?target a ?targetClass .
      ?target ext:tracesLineageTo ?agenda .
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?s a nfo:FileDataObject .
      ?target ext:file ?s.

      OPTIONAL {
        ?second <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource> ?s.
      }

      ${extraFilters}
    }
  }`;
  return queryEnv.run(query, true);
};

const cleanup = (queryEnv) => {
  const query = `
  DROP SILENT GRAPH <${queryEnv.tempGraph}>`;
  return queryEnv.run(query, true);
};

const fillOutDetailsOnVisibleItemsLeft = (queryEnv) => {
	const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a ?thing.
      ?s ?p ?o.
      ?s ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    { SELECT ?s ?p ?o ?thing ?agenda WHERE {
    GRAPH <${queryEnv.tempGraph}> {
			?s a ?thing .
			?s ext:tracesLineageTo ?agenda .
		}
	  
		GRAPH <${queryEnv.adminGraph}> {
			?s a ?thing.
			?s ?p ?o.
		}
		FILTER NOT EXISTS {
		  GRAPH <${queryEnv.tempGraph}> {
        ?s a ?thing.
        ?s ?p ?o.
      }
		}
		} LIMIT ${batchSize} }
  }`;
	return queryEnv.run(query, true);
};

const fillOutDetailsOnVisibleItemsRight = (queryEnv) => {
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?oo ?pp ?s.
    }
  } WHERE {
    { SELECT ?s ?pp ?oo WHERE {
    GRAPH <${queryEnv.tempGraph}> {
			?s a ?thing .
		}
	  
		GRAPH <${queryEnv.adminGraph}> {
			?s a ?thing.
			?oo ?pp ?s.
			
			FILTER NOT EXISTS {
			  GRAPH <${queryEnv.tempGraph}> {
			    ?oo ?pp ?s.
			  }
			}
		}
		} LIMIT ${batchSize} }
  }`;
  return queryEnv.run(query, true);
};

const repeatUntilTripleCountConstant = async function(fun, queryEnv, previousCount, graph){
	const funResult = await fun();
	graph = graph || queryEnv.tempGraph;
	const query = `SELECT (COUNT(?s) AS ?count) WHERE {
    GRAPH <${graph}> {
      ?s ?p ?o.
    }
  }`;
	return queryEnv.run(query, true).then((result) => {
		let count = 0;
		try {
			count = Number.parseInt(JSON.parse(result).results.bindings[0].count.value);
			console.log(`<${graph}> size is now ${count}...`);
		}catch (e) {
			console.log('no matching results');
		}
		if(count == previousCount){
			return funResult;
		}else {
			return repeatUntilTripleCountConstant(fun, queryEnv, count, graph);
		}
	});
};

const fillOutDetailsOnVisibleItems = (queryEnv) => {
	return Promise.all([
		repeatUntilTripleCountConstant(() => {
			return fillOutDetailsOnVisibleItemsLeft(queryEnv);
		}, queryEnv, 0),
		repeatUntilTripleCountConstant(() => {
			return fillOutDetailsOnVisibleItemsRight(queryEnv);
		}, queryEnv, 0)
	]);
};

const addAllRelatedDocuments = (queryEnv, extraFilters) => {
	extraFilters = extraFilters || '';
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a ?thing .
      ?version a ?subthing .
      ?s ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?target a ?targetClass .
      ?target ext:tracesLineageTo ?agenda .
    }
    GRAPH <${queryEnv.adminGraph}> {
      VALUES (?thing) {
        (foaf:Document) (ext:DocumentVersie)
      }
      ?s a ?thing .
      { { ?target ?p ?s . } 
        UNION
        { ?target ?p ?version .
          ?s <http://data.vlaanderen.be/ns/besluitvorming#heeftVersie> ?version .
        }
      }

      ${extraFilters}

      OPTIONAL {
        ?s besluitvorming:heeftVersie ?version.
        ?version a ?subthing.
      }
    }
  }`;
  return queryEnv.run(query, true);
};

const addAllRelatedToAgenda = (queryEnv, extraFilters) => {
	extraFilters = extraFilters || '';
  const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a ?thing .
      ?s ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?agenda a besluitvorming:Agenda .
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?s a ?thing .
      { { ?s ?p ?agenda } 
        UNION 
        { ?agenda ?p ?s } 
        UNION
        { ?agenda dct:hasPart ?agendaItem .
          ?s besluitvorming:isGeagendeerdVia ?agendaItem .
        }
      }
      FILTER( ?thing NOT IN(besluitvorming:Agenda) )
      ${extraFilters}
    }
  }`;
  return queryEnv.run(query, true);
};

const addRelatedToAgendaItemAndSubcase = (queryEnv, extraFilters) => {
	extraFilters = extraFilters || '';

	const query = `
   PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
   PREFIX dct: <http://purl.org/dc/terms/>
   PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
   PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
   PREFIX dbpedia: <http://dbpedia.org/ontology/>
   PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
   PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
   PREFIX foaf: <http://xmlns.com/foaf/0.1/>
   PREFIX schema: <http://schema.org>
   INSERT {
     GRAPH <${queryEnv.tempGraph}> {
       ?s a ?thing .
       ?s ext:tracesLineageTo ?agenda .
     }
   } WHERE {
     { SELECT ?target WHERE {
       GRAPH <${queryEnv.tempGraph}> {
         ?target ext:tracesLineageTo ?agenda .
         ?target a ?targetClass .
         FILTER(?targetClass IN (besluit:Agendapunt, dbpedia:UnitOfWork))
       }
     }}
     GRAPH <${queryEnv.adminGraph}> {
       ?s a ?thing .
       { { ?s [] ?target . } UNION { ?target [] ?s . } }
       FILTER( ?thing NOT IN (
         besluitvorming:Agenda,
         besluit:Agendapunt,
         dbpedia:UnitOfWork,
         foaf:Document,
         ext:DocumentVersie,
         nfo:FileDataObject ) )

      ${extraFilters}
    }
  }`;
  return queryEnv.run(query, true);
};

const runStage = async function(message, queryEnv, stage){
	let stageStart = moment().utc();
	await stage();
	logStage(stageStart, message, queryEnv.targetGraph);
};

const removeThingsWithLineageNoLongerInTempBatched = async function(queryEnv, targetedAgendas){
	if(!targetedAgendas){
		return;
	}
	const query = `
		PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    DELETE {
		  GRAPH <${queryEnv.targetGraph}> {
        ?s ?p ?o .
		    ?oo ?pp ?s .
		  }
		} WHERE {
		  { SELECT ?s ?p ?o ?oo ?pp WHERE {
		  VALUES (?agenda) {
		    (<${targetedAgendas.join('>) (<')}>)
		  }
		  GRAPH <${queryEnv.targetGraph}> {
		    ?s ext:tracesLineageTo ?agenda .
		    ?s ?p ?o .
		    OPTIONAL {
		      ?oo ?pp ?s .
		    }
		    FILTER ( ?p != ext:tracesLineageTo )
		  }
		  FILTER NOT EXISTS {
		    GRAPH <${queryEnv.tempGraph}> {
		      ?s ext:tracesLineageTo ?agenda .
		    }
		  }
		  } LIMIT ${batchSize} }
		}
		`;
	await queryEnv.run(query);
};

const removeLineageWhereLineageNoLongerInTempBatched = async function(queryEnv, targetedAgendas){
	if(!targetedAgendas){
		return;
	}
	const query = `
		PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    DELETE {
		  GRAPH <${queryEnv.targetGraph}> {
		    ?s ext:tracesLineageTo ?agenda .
		  }
		} WHERE {
		  { SELECT ?s ?agenda WHERE {
		  VALUES (?agenda) {
		    (<${targetedAgendas.join('>) (<')}>)
		  }
		  GRAPH <${queryEnv.targetGraph}> {
		    ?s ext:tracesLineageTo ?agenda .
		  }
		  FILTER NOT EXISTS {
		    GRAPH <${queryEnv.tempGraph}> {
		      ?s ext:tracesLineageTo ?agenda .
		    }
		  }
		  } LIMIT ${batchSize} }
		}
		`;
	await queryEnv.run(query);
};

const removeThingsWithLineageNoLongerInTemp = async function(queryEnv, targetedAgendas){
	await repeatUntilTripleCountConstant(() => {
		return removeThingsWithLineageNoLongerInTempBatched(queryEnv,targetedAgendas);
	}, queryEnv, 0, queryEnv.targetGraph);
	await repeatUntilTripleCountConstant(() => {
		return removeLineageWhereLineageNoLongerInTempBatched(queryEnv, targetedAgendas);
	}, queryEnv, 0, queryEnv.targetGraph);
};

const copyTempToTarget = async function(queryEnv){
	return repeatUntilTripleCountConstant(() => {
		return copySetOfTempToTarget(queryEnv);
	}, queryEnv, 0, queryEnv.targetGraph);
};

const copySetOfTempToTarget = async function(queryEnv){
	const query = `
		PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    INSERT {
		  GRAPH <${queryEnv.targetGraph}> {
        ?s ?p ?o .
		  }
		} WHERE {
		  GRAPH <${queryEnv.tempGraph}> {
        ?s a ?thing .
        { SELECT ?s ?p ?o WHERE {
          GRAPH <${queryEnv.tempGraph}> {
            ?s ?p ?o.
            FILTER NOT EXISTS {
              GRAPH <${queryEnv.targetGraph}> {
                ?s ?p ?o.
              }
            }
          }
        } LIMIT ${batchSize} }
      }
    }`;
	await queryEnv.run(query);
};

const removeStalePropertiesOfLineageBatch = async function(queryEnv, targetedAgendas){
	if(!targetedAgendas){
		return;
	}
	const query = `
		PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    DELETE {
		  GRAPH <${queryEnv.targetGraph}> {
        ?s ?p ?o .
		    ?oo ?pp ?s .
		  }
		} WHERE {
		  { SELECT ?s ?p ?o ?pp ?oo WHERE {
		    VALUES (?agenda) {
		      (<${targetedAgendas.join('>) (<')}>)
		    }
		    GRAPH <${queryEnv.tempGraph}> {
          ?s ext:tracesLineageTo ?agenda .
        }
  		  GRAPH <${queryEnv.targetGraph}> {
	  	    ?s ext:tracesLineageTo ?agenda .
  		  }
	  	  { {
		      GRAPH <${queryEnv.targetGraph}> {
    		    ?s ?p ?o .
    		  }
    		  FILTER NOT EXISTS {
		        GRAPH <${queryEnv.tempGraph}> {
		          ?s ?p ?o.
		        }
  		    }
	  	  } UNION {
		      GRAPH <${queryEnv.targetGraph}> {
            ?oo ?pp ?s .
          }
          FILTER NOT EXISTS {
            GRAPH <${queryEnv.tempGraph}> {
              ?oo ?pp ?s.
            }
          }
  		} } } LIMIT ${batchSize} }
		}
		`;
	await queryEnv.run(query);

};

const removeStalePropertiesOfLineage = async function(queryEnv, targetedAgendas) {
  return repeatUntilTripleCountConstant(() => {
  	return removeStalePropertiesOfLineageBatch(queryEnv, targetedAgendas);
	}, queryEnv, 0, queryEnv.targetGraph);
};

const cleanupBasedOnLineage = async function(queryEnv, targetedAgendas){
	await removeThingsWithLineageNoLongerInTemp(queryEnv, targetedAgendas);
	await removeStalePropertiesOfLineage(queryEnv, targetedAgendas);
};

const filterAgendaMustBeInSet = function(subjects, agendaVariable = "s"){
	if(!subjects || !subjects.length){
		return "";
	}
	return `VALUES (?${agendaVariable}) {(<${subjects.join('>) (<')}>)}`;
};

const generateTempGraph = async function(queryEnv){
	const tempGraph = `http://mu.semte.ch/temp/${mu.uuid()}`;
	queryEnv.tempGraph = tempGraph;
	await queryEnv.run(`
	PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    INSERT DATA {
	  GRAPH <${tempGraph}> {
	    <${tempGraph}> a ext:TempGraph .
	  }
	}`);
};

module.exports = {
	parseSparQlResults,
	removeInfoNotInTemp,
	notConfidentialFilter,
	notBeperktOpenbaarFilter,
	notInternOverheidFilter,
	addRelatedFiles,
	cleanup,
	fillOutDetailsOnVisibleItems,
	addAllRelatedDocuments,
	addAllRelatedToAgenda,
	addRelatedToAgendaItemAndSubcase,
	cleanupBasedOnLineage,
	logStage,
	filterAgendaMustBeInSet,
	generateTempGraph,
	copyTempToTarget,
	runStage
};

