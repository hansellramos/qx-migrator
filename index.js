console.log("e-Register migrator to Qualitrix");

var connectionParams = {
	eregister : {
	  host     : 'localhost',
	  port     : 3306,
	  user     : 'root',
	  password : '',
	  database : 'eregister'
	},
	qualitrix : {
	  host     : 'localhost',
	  port     : 27017,
	  database : 'qx'
	}
};

var mysql = require('mysql');
var Db = require('mongodb').Db,
    MongoClient = require('mongodb').MongoClient,
    Server = require('mongodb').Server;

var eregister = mysql.createConnection(connectionParams.eregister);

var qualitrix = new Db(connectionParams.qualitrix.database
	, new Server(
		connectionParams.qualitrix.host, connectionParams.qualitrix.port));

var queries = {
	eregister : {
		profile : 'SELECT r.cod_roll AS id, r.nom_roll AS name, r.descripcion AS description FROM roll AS r'
		, user : 'SELECT u.cod_usuario AS id, u.cod_roll AS profile, u.usuario AS username, u.pass AS password, u.nom_usuario AS firstname, TRIM(CONCAT(u.apellido1, " ",u.apellido2)) AS lastname, u.habil AS active FROM usuario AS u'
		, store: 'SELECT c.cod_categoria id, c.nom_categoria name, c.cod_categoria_ext reference, c.habil active FROM categorias c'
		, product: 'SELECT p.cod_producto id, p.cod_categoria store, p.nom_producto name, p.cod_producto_ext reference, p.habil active FROM productos p LIMIT 2'
		, property: 'SELECT c.cod_caracteristica id, c.nom_caracteristica name FROM caracteristicas c INNER JOIN detalle_producto dp ON dp.cod_caracteristica = c.cod_caracteristica WHERE dp.cod_producto = ?'
	}	
};

startProcess();

function startProcess(){
	console.log('Iniciando ejecución');
	openQualitrixConnection(function(qxdb){
		openEregisterConnection(function(error){
			if(error){
				closeQualitrixConnection(qxdb, endProcess);
			}else
			populateQualitrixToken(qxdb, function(error){
				if(error){
					closeEregisterConnection(function(){
						closeQualitrixConnection(qxdb, endProcess);
					});
				}else
				populateQualitrixProfile(qxdb, function(error){
					if(error){
						closeEregisterConnection(function(){
							closeQualitrixConnection(qxdb, endProcess);
						});
					}else
					populateQualitrixUser(qxdb, function(error){
						if(error){
							closeEregisterConnection(function(){
								closeQualitrixConnection(qxdb, endProcess);
							});
						}else
						populateQualitrixSubsidiary(qxdb, function(error){
							if(error){
								closeEregisterConnection(function(){
									closeQualitrixConnection(qxdb, endProcess);
								});
							}else
							populateQualitrixStore(qxdb, function(error){
								if(error){
									closeEregisterConnection(function(){
										closeQualitrixConnection(qxdb, endProcess);
									});
								}else
								populateQualitrixProducts(qxdb, function(error){
									if(error){
										closeEregisterConnection(function(){
											closeQualitrixConnection(qxdb, endProcess);
										});
									}else
									closeEregisterConnection(function(){
										closeQualitrixConnection(qxdb, endProcess);
									});
								});
							});
						});
					});
				});
			});
		});
	})
}

function endProcess(){
	console.log('Terminando ejecución');
	process.exit(1);
}

function openQualitrixConnection(cb){
	console.log('Conectando a Qualitrix...');
	qualitrix.open(function(err, qxdb) {
		if(err){
			console.log('...No ha sido posible conectar Qualitrix, terminando ejecución');
			process.exit(1);
		}else{
			console.log('...Conectado con Qualitrix');
			cb(qxdb);
		}
	});
}

function closeQualitrixConnection(qxdb, cb){
	console.log('Desconectando de Qualitrix...');
	setTimeout(function(){
		qxdb.close(function(error){
			console.log('...Desconectando exitosamente de Qualitrix');
			cb();
		});
	},1000);
}

function openEregisterConnection(cb){
	console.log('Conectando a e-Register...');
	eregister.connect(function(error) {
	 	if (error) {
	 		console.log('...No ha sido posible conectar e-Register: ' + error.stack);
	  	}else{
	  		console.log('...Conectado con e-Register');
	  	}
	  	cb(error);
	});
}

function closeEregisterConnection(cb){
	console.log('Desconectando de e-Register...');
	setTimeout(function(){
		eregister.end(function(error){
			console.log('...Desconectando exitosamente de e-Register');
			cb();
		});
	},1000);
}

function populateQualitrixToken(qxdb, cb){
	qxdb.collection('token').drop(function(error){
		console.log('  Insertando en tabla token...');
		qxdb.collection('token').insert({
			id: qxdb.collection('token').count()
	        , token: '=='
	        , user: 0
	        , iat: (new Date()).getTime()-3600
	        , expires: (new Date()).getTime() 
	        , created: (new Date()).getTime()
	        , creator: 0
	        , modified: (new Date()).getTime()
	        , modifier: 0
	        , deleted: false
	        , deleter: false
		}, function(error, doc){
	    	if(error){
	    		console.log('  ...Error insertando en tabla token');
	    	}else{
	    		console.log('  ...Tabla token poblada exitosamente');
	    	}
	    	cb(error);
	    });
    });
}

function populateQualitrixProfile(qxdb, cb){
	console.log('  Insertando en tabla profile...');
	console.log('    Consultando datos de table roll en e-Register...');
	eregister.query(queries.eregister.profile
		, function(error, results, fields){
			if(error){
				console.log('    ...No se han podido obtener datos de e-Register');
				cb(error);
			}
			else{
				console.log('    ...Se han obtenido '+results.length+' registros de e-Register');
				var items = [];
				for(var i = 0; i < results.length;i++){
					var result = results[i];
					items.push({
						id: result.id
						, name: result.name
						, description: result.description
						, active: true
						, created: (new Date()).getTime()
				        , creator: 0
				        , modified: (new Date()).getTime()
				        , modifier: 0
				        , deleted: false
				        , deleter: false
					});
				}
				console.log('    ...Insertando datos en Qualitrix');
				qxdb.collection('profile').drop(function(error){
					qxdb.collection('profile').insert(items, function(error, doc){
				    	if(error){
				    		console.log('    ...Error insertando en tabla profile');
				    		cb(error);
				    	}else{
				    		console.log('    ...Tabla profile poblada exitosamente');
				    		cb();
				    	}
				    });
				});
			}
		}
	);
}

function populateQualitrixUser(qxdb, cb){
	console.log('  Insertando en tabla user...');
	console.log('    Consultando datos de table usuarios en e-Register...');
	eregister.query(queries.eregister.user
		, function(error, results, fields){
			if(error){
				console.log('    ...No se han podido obtener datos de e-Register');
				cb(error);
			}
			else{
				console.log('    ...Se han obtenido '+results.length+' registros de e-Register');
				var items = [];
				for(var i = 0; i < results.length;i++){
					var result = results[i];
					items.push({
						id: result.id
						, profile: result.profile
						, username: result.username
						, password: result.password
						, firstname: result.firstname
						, lastname: result.lastname
						, active: result.active == 1
						, created: (new Date()).getTime()
				        , creator: 0
				        , modified: (new Date()).getTime()
				        , modifier: 0
				        , deleted: false
				        , deleter: false
					});
				}
				console.log('    ...Insertando datos en Qualitrix');
				qxdb.collection('user').drop(function(error){
					qxdb.collection('user').insert(items, function(error, doc){
				    	if(error){
				    		console.log('    ...Error insertando en tabla user');
				    		cb(error);
				    	}else{
				    		console.log('    ...Tabla user poblada exitosamente');
				    		cb();
				    	}
				    });
				});
			}
		}
	);
}

function populateQualitrixSubsidiary(qxdb, cb){
	qxdb.collection('subsidiary').drop(function(error){
		console.log('  Insertando en tabla subsidiary...');
		qxdb.collection('subsidiary').insert({
	        id: 1
	        , name: 'Barranquilla'
	        , reference: 3
	        , active: true
	        , created: (new Date()).getTime()
	        , creator: 0
	        , modified: (new Date()).getTime()
	        , modifier: 0
	        , deleted: false
	        , deleter: false
	    }, function(error, doc){
	    	if(error){
	    		console.log('  ...Error insertando en tabla subsidiary');
	    	}else{
	    		console.log('  ...Tabla subsidiary poblada exitosamente');
	    	}
	    	cb(error);
	    });
	});
}

function populateQualitrixStore(qxdb, cb){
	console.log('  Consultando subsidiary Barranquilla...');
	qxdb.collection('subsidiary').findOne({reference:3}, function(error, subsidiary){
		if(error){
			console.log('  ...Error Consultando Subsidiary Barranquilla');
		}else{
			console.log('    Consultando datos de table categorias en e-Register...');
			eregister.query(queries.eregister.store
				, function(error, results, fields){
					if(error){
						console.log('    ...No se han podido obtener datos de e-Register');
						cb(error);
					}
					else{
						console.log('    ...Se han obtenido '+results.length+' registros de e-Register');
						var items = [];						
						for(var i = 0; i < results.length;i++){
							var result = results[i];
							items.push({
								id: result.id
								, subsidiary: subsidiary.id
								, name: result.name
								, reference: result.reference
								, active: result.active == 1
								, created: (new Date()).getTime()
								, creator: 0
								, modified: (new Date()).getTime()
								, modifier: 0
								, deleted: false
								, deleter: false
							});
						}
						console.log('    ...Insertando datos en Qualitrix');
						qxdb.collection('store').drop(function(error){
							qxdb.collection('store').insert(items, function(error, doc){
								if(error){
									console.log('    ...Error insertando en tabla store');
									cb(error);
								}else{
									console.log('    ...Tabla store poblada exitosamente');
									cb();
								}
							});
						});
					}
				}
			);
		}
	});
}

function populateQualitrixProducts(qxdb, cb){
	console.log('    Consultando datos de table productos en e-Register...');
	eregister.query(queries.eregister.product
		, function(error, results, fields){
			if(error){
				console.log('    ...No se han podido obtener datos de e-Register');
				cb(error);
			}
			else{
				console.log('    ...Se han obtenido '+results.length+' registros de e-Register');
				var items = [];						
				for(var i = 0; i < results.length;i++){
					var result = results[i];
					console.log('      Consultando propiedades del producto '+result.name+' en e-Register...');
					eregister.query(queries.eregister.property, [result.id]
					, function(error, properties, fields){
						if(error){
							console.log('      ...No se han podido obtener datos de e-Register');
							cb(error);
						}
						else{
							console.log('      ...Se han obtenido '+properties.length+' propiedades de e-Register para el producto '+result.name);
							var p = [];
							for(var j = 0; j < properties.length;j++){
								var property = properties[j];
								p.push({
									id:property.id
									, name:property.name
									, active: true
									, created: (new Date()).getTime()
									, creator: 0
									, modified: (new Date()).getTime()
									, modifier: 0
									, deleted: false
									, deleter: false
								});
							}
							items.push({
								id: result.id
								, store: result.store
								, name: result.name
								, reference: result.reference
								, properties: p
								, active: result.active == 1
								, created: (new Date()).getTime()
								, creator: 0
								, modified: (new Date()).getTime()
								, modifier: 0
								, deleted: false
								, deleter: false
							});
							console.log('    ...Insertando datos en Qualitrix');
							qxdb.collection('product').drop(function(error){
								qxdb.collection('product').insert(items, function(error, doc){
									if(error){
										console.log('    ...Error insertando en tabla product');
										cb(error);
									}else{
										console.log('    ...Tabla product poblada exitosamente');
										cb();
									}
								});
							});
						}
					});
				}
			}
		}
	);
}