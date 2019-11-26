import mu from 'mu';
import moment from 'moment';
import {query} from './direct-sparql-endpoint';

const batchSize = process.env.BATCH_SIZE || 3000;
const smallBatchSize = process.env.SMALL_BATCH_SIZE || 100;
const minimalBatchSize = process.env.MINIMAL_BATCH_SIZE || 100;

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

const notInternRegeringFilter = `
    FILTER NOT EXISTS {
      ?s ?accessPredicate <http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/d335f7e3-aefd-4f93-81a2-1629c2edafa3> .
      VALUES (?accessPredicate ) {
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorProcedurestap> ) 
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDocument> )
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDocumentVersie> )
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDossier> )
      }
    }
`;

const notInternOverheidFilter = `
    FILTER NOT EXISTS {
      ?s ?accessPredicate ?levelTooTough .
      VALUES (?levelTooTough) {
        ( <http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/abe4c18d-13a9-45f0-8cdd-c493eabbbe29> )
        ( <http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/d335f7e3-aefd-4f93-81a2-1629c2edafa3> ) .
      }
      VALUES (?accessPredicate ) {
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorProcedurestap> ) 
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDocument> )
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDocumentVersie> )
        ( <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDossier> )
      }
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

const fillOutDetailsOnVisibleItemsLeft = async (queryEnv) => {
	const result = await queryEnv.run(`PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT DISTINCT ?s WHERE {
		  GRAPH <${queryEnv.tempGraph}> {
        ?s ext:tracesLineageTo ?agenda .
      }
      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s ext:yggdrasilLeft ?s.
        }
      }
    } LIMIT ${batchSize}`, true);
	const targets = JSON.parse(result).results.bindings.map((binding) => {
		return binding.s.value;
	});
	if(targets.length == 0){
		return;
	}

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
      ?s ?p ?o.
      ?s ext:yggdrasilLeft ?s.
    }
  } WHERE {
    VALUES ( ?s ) {
       ( <${targets.join('>) (<')}> )
    }
    GRAPH <${queryEnv.tempGraph}> {
		   ?s a ?thing .
		   ?s ext:tracesLineageTo ?agenda .
	  }
	  
		GRAPH <${queryEnv.adminGraph}> {
			?s ?p ?o.
		}
  }`;
	return queryEnv.run(query, true);
};

const fillOutDetailsOnVisibleItemsRight = async (queryEnv) => {
	const result = await queryEnv.run(`PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT DISTINCT ?s WHERE {
		  GRAPH <${queryEnv.tempGraph}> {
        ?s ext:tracesLineageTo ?agenda .
      }
      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s ext:yggdrasilRight ?s.
        }
      }
    } LIMIT ${batchSize}`, true);
	const targets = JSON.parse(result).results.bindings.map((binding) => {
		return binding.s.value;
	});
	if(targets.length == 0){
		return;
	}

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
      ?s ext:yggdrasilRight ?s.
    }
  } WHERE {
		VALUES ( ?s ) {
       ( <${targets.join('>) (<')}> )
    }
    GRAPH <${queryEnv.tempGraph}> {
			?s a ?thing .
		}
	
		GRAPH <${queryEnv.adminGraph}> {
			?oo ?pp ?s.
		}
  }`;
  return queryEnv.run(query, true);
};

const repeatUntilTripleCountConstant = async function(fun, queryEnv, previousCount, graph){
	const startQ = moment();
	const funResult = await fun();
	const timeQuery = moment().diff(startQ, 'seconds', true).toFixed(3);
	graph = graph || queryEnv.tempGraph;
	const start = moment();
	const query = `SELECT (COUNT(?s) AS ?count) WHERE {
    GRAPH <${graph}> {
      ?s ?p ?o.
    }
  }`;
	return queryEnv.run(query, true).then((result) => {
		const timeCount = moment().diff(start, 'seconds', true).toFixed(3);
		let count = 0;
		try {
			count = Number.parseInt(JSON.parse(result).results.bindings[0].count.value);
			console.log(`<${graph}> size is now ${count}... -- q: ${timeQuery}s, t: ${timeCount}s`);
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

const addAllRelatedDocuments = async (queryEnv, extraFilters) => {
	extraFilters = extraFilters || '';
  const queryTemplate = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a ext:DocumentVersie .
      ?s ext:tracesLineageTo ?agenda .
      ?document a foaf:Document .
      ?document ext:tracesLineageTo ?agenda .
    }
  } WHERE {
    { SELECT ?target ?agenda WHERE { 
      GRAPH <${queryEnv.tempGraph}> {
        ?target a ?targetClass .
        ?target ext:tracesLineageTo ?agenda .
      }
    } }
    GRAPH <${queryEnv.adminGraph}> {
      $REPLACECONSTRAINT
      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s a ext:DocumentVersie .
        }
      }   
      OPTIONAL {
        ?document besluitvorming:heeftVersie ?s .
      }

      ?s <http://mu.semte.ch/vocabularies/ext/toegangsniveauVoorDocumentVersie> ?anyAccessLevel .

      ${extraFilters}

    }
  }`;
  const constraints = [`
		?s a ext:DocumentVersie .
		?target ( ext:bevatDocumentversie | ext:zittingDocumentversie | ext:bevatReedsBezorgdeDocumentversie | ext:bevatAgendapuntDocumentversie | ext:bevatReedsBezorgdAgendapuntDocumentversie | ext:mededelingBevatDocumentversie | ext:documentenVoorPublicatie | ext:documentenVoorBeslissing | ext:getekendeDocumentVersiesVoorNotulen | dct:hasPart | prov:generated ) ?s .
  `,`
    ?s a ext:DocumentVersie .
    ?target (dct:hasPart | ext:beslissingsfiche | ext:getekendeNotulen ) / besluitvorming:heeftVersie ?s .
  `];

	await queryEnv.run(queryTemplate.split('$REPLACECONSTRAINT').join(constraints[0]), true);
	await queryEnv.run(queryTemplate.split('$REPLACECONSTRAINT').join(constraints[1]), true);
};

const addAllRelatedToAgenda = (queryEnv, extraFilters, relationProperties) => {
	relationProperties = relationProperties || ['dct:hasPart', 'ext:mededeling', 'besluit:isAangemaaktVoor', '^besluitvorming:behandelt', '( dct:hasPart / ^besluitvorming:isGeagendeerdVia )'];
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
      ?agenda ( ${relationProperties.join(" | ")} ) ?s .   
      ?s a ?thing .
      
      ${extraFilters}
    }
  }`;
  return queryEnv.run(query, true);
};

const addVisibleNotulen = (queryEnv, extraFilters) => {
	const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a ext:Notule .
      ?s ext:tracesLineageTo ?agenda.
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?agenda a besluitvorming:Agenda.
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?agenda besluit:isAangemaaktVoor ?session.
      ?session ext:releasedDecisions ?date.
      
      { { 
        ?session ext:algemeneNotulen ?s  .
        } UNION {
        ?agenda dct:hasPart / ext:notulenVanAgendaPunt ?s .
      } }

      ${extraFilters}
    }
  }`;
	return queryEnv.run(query, true);
};

const addRelatedToAgendaItemBatched = async (queryEnv, extraFilters) => {
	extraFilters = extraFilters || '';

	const query = `
   PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
   PREFIX dct: <http://purl.org/dc/terms/>
   PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
   PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
   PREFIX dbpedia: <http://dbpedia.org/ontology/>
   PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
   PREFIX prov: <http://www.w3.org/ns/prov#>
   PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
   PREFIX foaf: <http://xmlns.com/foaf/0.1/>
   PREFIX schema: <http://schema.org>
   SELECT ?s ?thing ?agenda WHERE {
     { SELECT ?target ?agenda WHERE {
       GRAPH <${queryEnv.tempGraph}> {
         ?target a besluit:Agendapunt .
         ?target ext:tracesLineageTo ?agenda .
       }
     }}
     GRAPH <${queryEnv.adminGraph}> {
       ?target (ext:subcaseAgendapuntFase | ext:bevatReedsBezorgdAgendapuntDocumentversie | ext:agendapuntGoedkeuring | ext:heeftVerdaagd | besluitvorming:opmerking ) ?s .  
       ?s a ?thing .
       
       FILTER NOT EXISTS {
         GRAPH <${queryEnv.tempGraph}> {
           ?s ext:tracesLineageTo ?agenda .
         }
       }
       
      ${extraFilters}
    }
  } LIMIT ${smallBatchSize}`;

	const result = await queryEnv.run(query, true);

	const targets = JSON.parse(result).results.bindings.map((binding) => {
		return `<${binding.s.value}> ext:tracesLineageTo <${binding.agenda.value}> .
<${binding.s.value}> a <${binding.thing.value}> .`;
	});

	if(targets.length < 1){
		return;
	}
	const update = `
   PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  
   INSERT DATA {
     GRAPH <${queryEnv.tempGraph}> {
       ${targets.join('\n')}
     }
   }`;
	return queryEnv.run(update, true);
};

const addRelatedToAgendaItem = async (queryEnv, extraFilters) => {
	await repeatUntilTripleCountConstant(() => {
		return addRelatedToAgendaItemBatched(queryEnv, extraFilters);
	}, queryEnv, 0, queryEnv.tempGraph);
};

const addRelatedToSubcaseBatched = async (queryEnv, extraFilters) => {
	extraFilters = extraFilters || '';

	let query = `
   PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
   PREFIX dct: <http://purl.org/dc/terms/>
   PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
   PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
   PREFIX dbpedia: <http://dbpedia.org/ontology/>
   PREFIX prov: <http://www.w3.org/ns/prov#>
   PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
   PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
   PREFIX foaf: <http://xmlns.com/foaf/0.1/>
   PREFIX schema: <http://schema.org>
   SELECT ?s ?thing ?agenda WHERE {
		 { SELECT ?target ?agenda WHERE {
		   GRAPH <${queryEnv.tempGraph}> {
			   ?target a dbpedia:UnitOfWork .
			   ?target ext:tracesLineageTo ?agenda .
		   }
		 }}

     GRAPH <${queryEnv.adminGraph}> {
       ?target ( ext:bevatReedsBezorgdeDocumentversie | ^dct:hasPart | ext:subcaseProcedurestapFase | ext:bevatConsultatievraag | ext:procedurestapGoedkeuring | besluitvorming:opmerking ) ?s .
       ?s a ?thing .
       
       FILTER NOT EXISTS {
         GRAPH <${queryEnv.tempGraph}> {
           ?s ext:tracesLineageTo ?agenda .
				 }
       }
       
       ${extraFilters}
    }
  } LIMIT ${smallBatchSize}`;

  const result = await queryEnv.run(query, true);

	const targets = JSON.parse(result).results.bindings.map((binding) => {
		return `<${binding.s.value}> ext:tracesLineageTo <${binding.agenda.value}> .
<${binding.s.value}> a <${binding.thing.value}> .`;
	});

  if(targets.length < 1){
		return;
  }
	const update = `
   PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  
   INSERT DATA {
     GRAPH <${queryEnv.tempGraph}> {
       ${targets.join('\n')}
     }
   }`;
	return queryEnv.run(update, true);
};

const addRelatedToSubcase = async (queryEnv, extraFilters) => {
	await repeatUntilTripleCountConstant(() => {
		return addRelatedToSubcaseBatched(queryEnv, extraFilters);
	}, queryEnv, 0, queryEnv.tempGraph);
};

const addRelatedToAgendaItemAndSubcase = async (queryEnv, extraFilters) => {
	return Promise.all([
		addRelatedToAgendaItem(queryEnv, extraFilters),
	  addRelatedToSubcase(queryEnv, extraFilters)
	]);
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

	const result = await queryEnv.run(`PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT DISTINCT ?s WHERE {
		  VALUES (?agenda) {
        (<${targetedAgendas.join('>) (<')}>)
      }
      GRAPH <${queryEnv.targetGraph}> {
        ?s ext:tracesLineageTo ?agenda .
      }
      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s ext:tracesLineageTo ?anyTargetedAgenda.
        }
      }
    } LIMIT ${smallBatchSize}`, true);
	  const targets = JSON.parse(result).results.bindings.map((binding) => {
		  return binding.s.value;
	  });
	  if(targets.length == 0){
  		return;
  	}

	  const queryRight = `
		PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    DELETE {
		  GRAPH <${queryEnv.targetGraph}> {
        ?s ?p ?o .
		  }
		} WHERE {
		  VALUES ( ?s ) {
		    ( <${targets.join('>) (<')}> )
		  }
		  GRAPH <${queryEnv.targetGraph}> {
		    ?s ?p ?o .
		  }
		}`;
	await queryEnv.run(queryRight);
	const queryLeft = `
		PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    DELETE {
		  GRAPH <${queryEnv.targetGraph}> {
		    ?oo ?pp ?s .
		  }
		} WHERE {
		  VALUES ( ?s ) {
		    ( <${targets.join('>) (<')}> )
		  }
		  GRAPH <${queryEnv.targetGraph}> {
		    ?oo ?pp ?s .
		  }
		}`;
	await queryEnv.run(queryLeft);
};

const removeLineageWhereLineageNoLongerInTempBatched = async function(queryEnv, targetedAgendas){
	if(!targetedAgendas){
		return;
	}

	const result = await queryEnv.run(`
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  SELECT ?s ?agenda WHERE {
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
	} LIMIT ${minimalBatchSize}
  `, true);

	const targets = JSON.parse(result).results.bindings.map((binding) => {
		return `<${binding.s.value}> ext:tracesLineageTo <${binding.agenda.value}> .`;
	});

  if(!targets || targets.length == 0){
		return;
  }
	const query = `
	PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  DELETE DATA {
		GRAPH <${queryEnv.targetGraph}> {
			${targets.join("\n")}
		}
	}`;
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
	const result = await queryEnv.run(`PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT DISTINCT ?s WHERE {
		  GRAPH <${queryEnv.tempGraph}> {
        ?s a ?thing .
      }
      FILTER NOT EXISTS {
        GRAPH <${queryEnv.tempGraph}> {
          ?s ext:yggdrasilMoved ?s .
        }
      }
    } LIMIT ${smallBatchSize}`, true);
	const targets = JSON.parse(result).results.bindings.map((binding) => {
		return binding.s.value;
	});
	if(targets.length == 0){
		return;
	}
	const query = `
		PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    INSERT {
		  GRAPH <${queryEnv.targetGraph}> {
        ?s ?p ?o .
      }
      GRAPH <${queryEnv.tempGraph}> {
        ?s ext:yggdrasilMoved ?s .
      }
		} WHERE {
      GRAPH <${queryEnv.tempGraph}> {
        VALUES (?s) {
          ( <${targets.join('>) (<')}> )
        }
				?s ?p ?o .
				FILTER (?p NOT IN ( ext:yggdrasilLeft, ext:yggdrasilRight ) )
      }
    }`;
	await queryEnv.run(query);
};

const removeStalePropertiesOfLineageBatch = async function(queryEnv, targetedAgendas){
	if(!targetedAgendas){
		return;
	}
	const result = await queryEnv.run(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT DISTINCT ?s WHERE {
      VALUES (?agenda) {
        (<${targetedAgendas.join('>) (<')}>)
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
      } }
    } LIMIT ${batchSize}`, true);
	const targets = JSON.parse(result).results.bindings.map((binding) => {
		return binding.s.value;
	});
	if(targets.length == 0){
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
		  VALUES (?s) {
		    ( <${targets.join('>) (<')}> )
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
		  } }
		}`;
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

const addVisibleDecisions = (queryEnv, extraFilters) => {
	const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a besluit:Besluit.
      ?s ext:tracesLineageTo ?agenda.
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?agendaitem a besluit:Agendapunt.
      ?agendaitem ext:tracesLineageTo ?agenda.
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?agenda dct:hasPart ?agendaitem.
      ?agenda besluit:isAangemaaktVoor ?session.
      ?session ext:releasedDecisions ?date.
      ?subcase besluitvorming:isGeagendeerdVia ?agendaitem.
      ?subcase ext:procedurestapHeeftBesluit ?s.
      ?s besluitvorming:goedgekeurd "true"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> .
      
      ${extraFilters}
    }
  }`;
	return queryEnv.run(query, true);
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
	}`, true);
};

const addVisibleNewsletterInfo = async (queryEnv, extraFilters) => {
	const query = `
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  INSERT {
    GRAPH <${queryEnv.tempGraph}> {
      ?s a besluitvorming:NieuwsbriefInfo .
      ?s ext:tracesLineageTo ?agenda.
    }
  } WHERE {
    GRAPH <${queryEnv.tempGraph}> {
      ?target a ?thing .
      ?target ext:tracesLineageTo ?agenda.
    }
    GRAPH <${queryEnv.adminGraph}> {
      ?s (^prov:generated | ^ext:algemeneNieuwsbrief ) ?target .
      
      ?agenda besluit:isAangemaaktVoor ?session .
      ?session ext:releasedDecisions ?date .
      ?session ext:heeftMailCampagnes / ext:isVerstuurdOp ?sentMailDate .
      
      ${extraFilters}
    }
  }`;
	return queryEnv.run(query, true);
};

const configurableQuery = function(queryString, direct){
	return query({
		sudo: true,
		url: direct?process.env.DIRECT_ENDPOINT:undefined
	}, queryString);
};

const directQuery = function(queryString){
	return configurableQuery(queryString, true);
};

module.exports = {
	parseSparQlResults,
	removeInfoNotInTemp,
	notConfidentialFilter,
	notInternRegeringFilter,
	notInternOverheidFilter,
	addRelatedFiles,
	cleanup,
	fillOutDetailsOnVisibleItems,
	addAllRelatedDocuments,
	addVisibleDecisions,
	addAllRelatedToAgenda,
	addVisibleNewsletterInfo,
	addRelatedToAgendaItemAndSubcase,
	cleanupBasedOnLineage,
	logStage,
	addVisibleNotulen,
	filterAgendaMustBeInSet,
	generateTempGraph,
	copyTempToTarget,
	configurableQuery,
	directQuery,
	runStage
};

