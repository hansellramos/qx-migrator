var log = "";
var processDate = new Date();

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

var fs = require('fs');
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
		, product: 'SELECT p.cod_producto id, p.cod_categoria store, p.nom_producto name, p.cod_producto_ext reference, p.habil active FROM productos p'
		, property: 'SELECT c.cod_caracteristica id, replace(c.nom_caracteristica,"\r","") name, dp.cod_producto product FROM caracteristicas c INNER JOIN detalle_producto dp ON dp.cod_caracteristica = c.cod_caracteristica ORDER BY dp.cod_producto'
		, external: 'SELECT c.cod_cliente id, c.nom_cliente name FROM clientes c UNION SELECT 1000+p.cod_proveedor id, p.nom_proveedor name FROM proveedores p'
		, record: 'SELECT m.cod_muestra id, m.cod_producto product, m.lote reference, m.fecha_analisis analysis_date,  m.fecha_elaboracion elaboration_date, m.fecha_vencimiento due_date, m.fecha_recepcion reception_date, m.cod_usuario user, m.desicion veredict, m.cumple satisfies, m.habil active, m.remision remission, m.cantidad quantity, m.cantidad_existente existing_quantity, 1000+m.cod_proveedor supplier, m.clausula clause, UNIX_TIMESTAMP(m.fecha_elaboracion)*1000 created FROM muestras m'
		, record_detail: 'SELECT dmp.cod_muestra record, dmp.cod_caracteristica property, dmp.valor value FROM detalle_muestra_producto dmp'
		, certificate: 'SELECT cod_certificado id, cod_cliente customer, cod_producto product, cod_usuario `creator`, UNIX_TIMESTAMP(STR_TO_DATE(CONCAT(fecha,hora),"%d/%m/%Y%H:%i"))*1000 `created`, remision remission, IFNULL(clausula,"N/A") clause, cantidad quantity, presentacion presentation FROM certificado'
		, certificate_properties: 'SELECT cod_certificado certificate, cod_encabezado id, replace(encabezado,"\r","") name FROM campos_certificado'
		, certificate_values: 'SELECT cod_certificado certificate, cod_encabezado property, lote record, valor value FROM propiedad_certificado ORDER BY certificate, record '
	}
	, counters: {
		profile: 'SELECT MAX(cod_roll)+1 counter FROM roll'
		, user: 'SELECT MAX(cod_usuario)+1 counter FROM usuario'
		, store: 'SELECT MAX(cod_categoria)+1 counter FROM categorias'
		, product: 'SELECT MAX(cod_producto)+1 counter FROM productos'
		, property: 'SELECT MAX(cod_caracteristica)+1 counter FROM caracteristicas'
		, external: 'SELECT MAX(cod_proveedor)+1000+1 counter FROM proveedores'
		, record: 'SELECT MAX(cod_muestra)+1 counter FROM muestras'
		, certificate: 'SELECT MAX(cod_certificado)+1 counter FROM certificado'
		, subsidiary: 'SELECT 1+1 counter'
	}	
};

function addToLog(string){
	var d = new Date();
	log += (1900+d.getYear())+(d.getMonth()<10?'0':'')+(d.getMonth()+1)+(d.getDate()<10?'0':'')+d.getDate()+d.getHours()+d.getMinutes()+(d.getSeconds()<10?'0':'')+d.getSeconds()+' :: '+string+"\r\n";
	return string;
}

function writeLog(cb){
	var d = processDate;
	fs.appendFile('log_'+(1900+d.getYear())+(d.getMonth()<10?'0':'')+(d.getMonth()+1)+(d.getDate()<10?'0':'')+d.getDate()+d.getHours()+d.getMinutes()+(d.getSeconds()<10?'0':'')+d.getSeconds()+'.log', log, function(error){
		if(error){
			console.log(addToLog(error));
		}
		log = '';
		if(cb){
			cb();	
		}		
	});
}

function startProcess(){
	console.log(addToLog('Iniciando ejecución'));
	openQualitrixConnection(function(qxdb){
		openEregisterConnection(function(error){
			if(error){
				closeQualitrixConnection(qxdb, endProcess);
			}else
				deleteCounters(qxdb, function(error){
				if(error){
					closeEregisterConnection(function(){
						closeQualitrixConnection(qxdb, endProcess);
					});
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
										populateQualitrixExternal(qxdb, function(error){
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
												populateQualitrixCertificate(qxdb, function(error){
												if(error){
													closeEregisterConnection(function(){
														closeQualitrixConnection(qxdb, endProcess);
													});
												}else
													populateQualitrixRecords(qxdb, function(error){
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
					});
				});
			});
		});
	})
}

function endProcess(){
	console.log(addToLog('Terminando ejecución'));
	writeLog(function(){
		process.exit(1);
	});
}

function openQualitrixConnection(cb){
	console.log(addToLog('Conectando a Qualitrix...'));
	qualitrix.open(function(err, qxdb) {
		if(err){
			console.log(addToLog('...No ha sido posible conectar Qualitrix, terminando ejecución'));
			endProcess();
		}else{
			console.log(addToLog('...Conectado con Qualitrix'));
			writeLog(function(){cb(qxdb);});
		}
	});
}

function closeQualitrixConnection(qxdb, cb){
	console.log(addToLog('Desconectando de Qualitrix...'));
	setTimeout(function(){
		qxdb.close(function(error){
			console.log(addToLog('...Desconectando exitosamente de Qualitrix'));
			writeLog(function(){cb(error);});
		});
	},1000);
}

function openEregisterConnection(cb){
	console.log(addToLog('Conectando a e-Register...'));
	eregister.connect(function(error) {
	 	if (error) {
	 		console.log(addToLog('...No ha sido posible conectar e-Register: ' + error.stack));
	  	}else{
	  		console.log(addToLog('...Conectado con e-Register'));
	  	}
	  	writeLog(function(){cb(error);});
	});
}

function closeEregisterConnection(cb){
	console.log(addToLog('Desconectando de e-Register...'));
	setTimeout(function(){
		eregister.end(function(error){
			console.log(addToLog('...Desconectando exitosamente de e-Register'));
			writeLog(function(){cb(error);});
		});
	},1000);
}

function deleteCounters(qxdb, cb){
	console.log(addToLog('Eliminando contadores'));
	qxdb.collection('counters').drop(function(error){
		if(error){
    		console.log(addToLog('...Error eliminando contadores en Qualitrix. '+error));
    	}else{
    		console.log(addToLog('...Contadores eliminados exitosamente'));
    	}
    	writeLog(function(){cb(error);});
	});
}

function populateQualitrixToken(qxdb, cb){
	console.log(addToLog('  Iniciando Proceso token...'));
	qxdb.collection('token').drop(function(error){
		console.log(addToLog('  Insertando en tabla token en Qualitrix...'));
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
	        , deleter: 0
		}, function(error, doc){
	    	if(error){
	    		console.log(addToLog('  ...Error insertando en tabla token en Qualitrix. '+error));
	    		writeLog(function(){cb(error);});
	    	}else{
	    		console.log(addToLog('  ...Tabla token poblada exitosamente'));
	    		console.log(addToLog('  Creando indices en tabla token...'));
	    		qxdb.collection('token').createIndex('token', {unique: true}, function(error){
    				if(error){
			    		console.log(addToLog('  ...Error insertando en tabla token en Qualitrix. '+error));
			    	}else{
			    		console.log(addToLog('  ...Indices creados exitosamente'));
			    	}
			    	console.log(addToLog('  Proceso token finalizado...'));
	    			writeLog(function(){cb(error);});
	    		});
	    	}
	    });
    });
}

function populateQualitrixProfile(qxdb, cb){
	console.log(addToLog('  Iniciando Proceso profile...'));
	console.log(addToLog('    Consultando datos de table roll en e-Register...'));
	eregister.query(queries.eregister.profile
		, function(error, results, fields){
			if(error){
				console.log(addToLog('    ...No se han podido obtener datos de e-Register'));
				writeLog(function(){cb(error);});
			}
			else{
				console.log(addToLog('    ...Se han obtenido '+results.length+' registros de e-Register'));
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
				        , deleter: 0
					});
				}
				console.log(addToLog('    Insertando datos en profile de Qualitrix...'));
				qxdb.collection('profile').drop(function(error){
					qxdb.collection('profile').insert(items, function(error, doc){
				    	if(error){
				    		console.log(addToLog('    ...Error insertando en tabla profile. '+error));
				    		writeLog(function(){cb(error);});
				    	}else{
				    		console.log(addToLog('    ...Tabla profile poblada exitosamente'));
				    		console.log(addToLog('    Creando indices en tabla profile...'));
				    		qxdb.collection('profile').createIndex('id', {unique: true}, function(error){
			    				if(error){
						    		console.log(addToLog('    ...Error insertando en tabla profile en Qualitrix. '+error));
						    		writeLog(function(){cb(error);});
						    	}else{
						    		console.log(addToLog('    ...Indices creados exitosamente'));
						    		console.log(addToLog('    Creando contadores en tabla profile...'));
						    		eregister.query(queries.counters.profile
					    			, function(error, results, fields){
					    				if(error){
				    						console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
											writeLog(function(){cb(error);});
					    				}else{
					    					qxdb.collection('counters').insert({
					    						name: 'profile',
					    						seq: results[0].counter,
					    						date: (new Date()).getTime()
					    					}, function(error, doc){
					    						if(error){
					    							console.log(addToLog('    ...Error insertando en tabla profile. '+error));
					    						}else{
					    							console.log(addToLog('    Proceso profile en finalizado...'));
					    						}
					    						writeLog(function(){cb(error);});
					    					});
					    				}
					    			});
						    	}
				    		});
				    	}
				    	
				    });
				});
			}
		}
	);
}

function populateQualitrixUser(qxdb, cb){
	console.log(addToLog('  Iniciando Proceso user...'));
	console.log(addToLog('    Consultando datos de table usuarios en e-Register...'));
	eregister.query(queries.eregister.user
		, function(error, results, fields){
			if(error){
				console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
				writeLog(function(){cb(error);});
			}
			else{
				console.log(addToLog('    ...Se han obtenido '+results.length+' registros de e-Register'));
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
				        , deleter: 0
					});
				}
				console.log(addToLog('    Insertando datos en user de Qualitrix...'));
				qxdb.collection('user').drop(function(error){
					qxdb.collection('user').insert(items, function(error, doc){
				    	if(error){
				    		console.log(addToLog('    ...Error insertando en tabla user. '+error));
				    		writeLog(function(){cb(error);});
				    	}else{
				    		console.log(addToLog('    ...Tabla user poblada exitosamente'));
				    		console.log(addToLog('    Creando indices en tabla user...'));
				    		qxdb.collection('user').createIndex('id', {unique: true}, function(error){
			    				if(error){
						    		console.log(addToLog('    ...Error insertando en tabla user en Qualitrix. '+error));
						    		writeLog(function(){cb(error);});
						    	}else{
						    		console.log(addToLog('    ...Indices creados exitosamente'));
						    		console.log(addToLog('    Creando contadores en tabla user...'));
						    		eregister.query(queries.counters.user
					    			, function(error, results, fields){
					    				if(error){
				    						console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
											writeLog(function(){cb(error);});
					    				}else{
					    					qxdb.collection('counters').insert({
					    						name: 'user',
					    						seq: results[0].counter,
					    						date: (new Date()).getTime()
					    					}, function(error, doc){
					    						if(error){
					    							console.log(addToLog('    ...Error insertando en tabla user. '+error));
					    						}else{
					    							console.log(addToLog('    Proceso user en finalizado...'));
					    						}
					    						writeLog(function(){cb(error);});
					    					});
					    				}
					    			});
						    	}
				    		});
				    	}
				    });
				});
			}
		}
	);
}

function populateQualitrixSubsidiary(qxdb, cb){
	console.log(addToLog('  Iniciando Proceso subsidiary...'));
	qxdb.collection('subsidiary').drop(function(error){
		console.log(addToLog('    Insertando en tabla subsidiary...'));
		qxdb.collection('subsidiary').insert({
	        id: 1
	        , name: 'Barranquilla'
	        , reference: '3'
	        , active: true
	        , created: (new Date()).getTime()
	        , creator: 0
	        , modified: (new Date()).getTime()
	        , modifier: 0
	        , deleted: false
	        , deleter: 0
	    }, function(error, doc){
	    	if(error){
	    		console.log(addToLog('    ...Error insertando en tabla subsidiary. '+error));
	    		writeLog(function(){cb(error);});
	    	}else{
	    		console.log(addToLog('    ...Tabla subsidiary poblada exitosamente'));
				console.log(addToLog('    Creando índices de tabla subsidiary en Qualitrix...'));
	    		qxdb.collection('subsidiary').createIndex('id', {unique: true}, function(error){
    				if(error){
			    		console.log(addToLog('    ...Error insertando en tabla subsidiary en Qualitrix. '+error));
			    		writeLog(function(){cb(error);});
			    	}else{
			    		console.log(addToLog('    ...Indices creados exitosamente'));
			    		console.log(addToLog('    Creando contadores en tabla subsidiary...'));
			    		eregister.query(queries.counters.subsidiary
		    			, function(error, results, fields){
		    				if(error){
	    						console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
								writeLog(function(){cb(error);});
		    				}else{
		    					qxdb.collection('counters').insert({
		    						name: 'subsidiary',
		    						seq: results[0].counter,
		    						date: (new Date()).getTime()
		    					}, function(error, doc){
		    						if(error){
		    							console.log(addToLog('    ...Error insertando en tabla subsidiary. '+error));
		    						}else{
		    							console.log(addToLog('    Proceso subsidiary en finalizado...'));
		    						}
		    						writeLog(function(){cb(error);});
		    					});
		    				}
		    			});
			    	}
	    		});
	    	}
	    });
	});
}

function populateQualitrixStore(qxdb, cb){
	console.log(addToLog('  Iniciando Proceso store...'));
	console.log(addToLog('    Consultando subsidiary Barranquilla...'));
	qxdb.collection('subsidiary').findOne({reference:'3'}, function(error, subsidiary){
		if(error){
			console.log(addToLog('    ...Error Consultando Subsidiary Barranquilla. '+error));
			writeLog(function(){cb(error);});
		}else{
			console.log(addToLog('    Consultando datos de table categorias en e-Register...'));
			eregister.query(queries.eregister.store
				, function(error, results, fields){
					if(error){
						console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
						writeLog(function(){cb(error);});
					}
					else{
						console.log(addToLog('    ...Se han obtenido '+results.length+' registros de e-Register'));
						var items = [];						
						for(var i = 0; i < results.length;i++){
							var result = results[i];
							items.push({
								id: result.id
								, subsidiary: subsidiary.id
								, name: result.name
								, reference: result.reference
								, address: ''
								, phone: ''
								, notes: ''
								, active: result.active == 1
								, created: (new Date()).getTime()
								, creator: 0
								, modified: (new Date()).getTime()
								, modifier: 0
								, deleted: false
								, deleter: 0
							});
						}
						console.log(addToLog('    Insertando datos en Qualitrix...'));
						qxdb.collection('store').drop(function(error){
							qxdb.collection('store').insert(items, function(error, doc){
								if(error){
									console.log(addToLog('    ...Error insertando en tabla store. '+error));
									writeLog(function(){cb(error);});
								}else{
									console.log(addToLog('    ...Tabla store poblada exitosamente'));
									qxdb.collection('store').createIndex('id', {unique: true}, function(error){
					    				if(error){
								    		console.log(addToLog('    ...Error insertando en tabla store en Qualitrix. '+error));
								    		writeLog(function(){cb(error);});
								    	}else{
								    		console.log(addToLog('    ...Indices creados exitosamente'));
								    		console.log(addToLog('    Creando contadores en tabla store...'));
								    		eregister.query(queries.counters.store
							    			, function(error, results, fields){
							    				if(error){
						    						console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
													writeLog(function(){cb(error);});
							    				}else{
							    					qxdb.collection('counters').insert({
							    						name: 'store',
							    						seq: results[0].counter,
							    						date: (new Date()).getTime()
							    					}, function(error, doc){
							    						if(error){
							    							console.log(addToLog('    ...Error insertando en tabla store. '+error));
							    						}else{
							    							console.log(addToLog('    Proceso store en finalizado...'));
							    						}
							    						writeLog(function(){cb(error);});
							    					});
							    				}
							    			});
								    	}
						    		});
								}
							});
						});
					}
				}
			);
		}
	});
}

function populateQualitrixExternal(qxdb, cb){
	console.log(addToLog('  Iniciando Proceso external...'));
	console.log(addToLog('    Consultando datos de table clientes y proveedores en e-Register...'));
	eregister.query(queries.eregister.external
		, function(error, results, fields){
			if(error){
				console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
				writeLog(function(){cb(error);});
			}
			else{
				console.log(addToLog('    ...Se han obtenido '+results.length+' registros de e-Register'));
				var items = [];						
				for(var i = 0; i < results.length;i++){
					var result = results[i];
					items.push({
						id: result.id
						, name: result.name
						, address: ''
						, phone: ''
						, notes: ''
						, contact: ''
						, active: true == 1
						, created: (new Date()).getTime()
						, creator: 0
						, modified: (new Date()).getTime()
						, modifier: 0
						, deleted: false
						, deleter: 0
					});
				}
				console.log(addToLog('    Insertando datos en Qualitrix...'));
				qxdb.collection('external').drop(function(error){
					qxdb.collection('external').insert(items, function(error, doc){
						if(error){
							console.log(addToLog('    ...Error insertando en tabla external. '+error));
							writeLog(function(){cb(error);});
						}else{
							console.log(addToLog('    ...Tabla external poblada exitosamente'));
							qxdb.collection('external').createIndex('id', {unique: true}, function(error){
			    				if(error){
						    		console.log(addToLog('    ...Error insertando en tabla external en Qualitrix. '+error));
						    		writeLog(function(){cb(error);});
						    	}else{
						    		console.log(addToLog('    ...Indices creados exitosamente'));
						    		console.log(addToLog('    Creando contadores en tabla external...'));
						    		eregister.query(queries.counters.external
					    			, function(error, results, fields){
					    				if(error){
				    						console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
											writeLog(function(){cb(error);});
					    				}else{
					    					qxdb.collection('counters').insert({
					    						name: 'external',
					    						seq: results[0].counter,
					    						date: (new Date()).getTime()
					    					}, function(error, doc){
					    						if(error){
					    							console.log(addToLog('    ...Error insertando en tabla external. '+error));
					    						}else{
					    							console.log(addToLog('    Proceso external en finalizado...'));
					    						}
					    						writeLog(function(){cb(error);});
					    					});
					    				}
					    			});
						    	}
				    		});
						}
					});
				});
			}
		}
	);

}

function populateQualitrixProducts(qxdb, cb){
	console.log(addToLog('  Iniciando Proceso product...'));
	console.log(addToLog('    Consultando datos de table productos en e-Register...'));
	eregister.query(queries.eregister.product
		, function(error, products, fields){
			if(error){
				console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
				writeLog(function(){cb(error);});
			}
			else{
				console.log(addToLog('    ...Se han obtenido '+products.length+' registros de e-Register'));
				console.log(addToLog('      Consultando propiedades en e-Register...'));
				eregister.query(queries.eregister.property
				, function(error, properties, fields){
					if(error){
						console.log(addToLog('      ...No se han podido obtener datos de e-Register. '+error));
						writeLog(function(){cb(error);});
					}
					else{
						console.log(addToLog('      ...Se han obtenido '+properties.length+' propiedades de e-Register'));
						console.log(addToLog('      Relacionando productos con propiedades...'));
						writeLog();
						var items = [];
						for(var i = 0; i < products.length; i++){
							var product = products[i];
							var _properties = [];
							for(var j = 0; j < properties.length; j++){
								var property = properties[j];
								if(product.id == property.product){
									_properties.push({
										id:property.id
										, name:property.name.replace(/(\\r)|((\<+\/*(html|HTML|head|HEAD|body|BODY|font|FONT)+([ a-zA-Z=\\"0-9]*)+\>))|( {2,})/g,"").trim()
										, validations:{
											type:'text'
										}
										, active: true
										, created: (new Date()).getTime()
										, creator: 0
										, modified: (new Date()).getTime()
										, modifier: 0
										, deleted: false
										, deleter: 0
									});
								}
							}
							addToLog('      ...Se han obtenido '+_properties.length+' propiedades de e-Register para el producto '+product.name);
							items.push({
								id: product.id
								, store: product.store
								, name: product.name
								, reference: product.reference
								, notes: ''
								, properties: _properties
								, active: product.active == 1
								, created: (new Date()).getTime()
								, creator: 0
								, modified: (new Date()).getTime()
								, modifier: 0
								, deleted: false
								, deleter: 0
							});
						}
						console.log(addToLog('      ...Relacion finalizada'));
						writeLog();
						console.log(addToLog('    Insertando datos en Qualitrix...'));
						qxdb.collection('product').drop(function(error){
							qxdb.collection('product').insert(items, function(error, doc){
								if(error){
									console.log(addToLog('    ...Error insertando en tabla product. '+error));
									writeLog(function(){cb(error);});
								}else{
									console.log(addToLog('    ...Tabla product poblada exitosamente'));
									qxdb.collection('product').createIndex('id', {unique: true}, function(error){
					    				if(error){
								    		console.log(addToLog('    ...Error insertando en tabla product en Qualitrix. '+error));
								    		writeLog(function(){cb(error);});
								    	}else{
								    		console.log(addToLog('    ...Indices creados exitosamente'));
								    		console.log(addToLog('    Creando contadores en tabla profile...'));
								    		eregister.query(queries.counters.profile
							    			, function(error, results, fields){
							    				if(error){
						    						console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
													writeLog(function(){cb(error);});
							    				}else{
							    					qxdb.collection('counters').insert({
							    						name: 'product',
							    						seq: results[0].counter,
							    						date: (new Date()).getTime()
							    					}, function(error, doc){
							    						if(error){
							    							console.log(addToLog('    ...Error insertando en tabla product. '+error));
							    							writeLog(function(){cb(error);});
							    						}else{
							    							console.log(addToLog('    Creando contadores en tabla property...'));
												    		eregister.query(queries.counters.property
											    			, function(error, results, fields){
											    				if(error){
										    						console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
																	writeLog(function(){cb(error);});
											    				}else{
											    					qxdb.collection('counters').insert({
											    						name: 'property',
											    						seq: results[0].counter,
											    						date: (new Date()).getTime()
											    					}, function(error, doc){
											    						if(error){
											    							console.log(addToLog('    ...Error insertando en tabla property. '+error));
											    						}else{
											    							console.log(addToLog('    Proceso product en finalizado...'));
											    						}
											    						writeLog(function(){cb(error);});
											    					});
											    				}
											    			});
							    						}
							    					});
							    				}
							    			});
								    	}
						    		});
								}
							});
						});
					}
				});
			}
		}
	);
}

function populateQualitrixRecords(qxdb, cb){
	console.log(addToLog('  Iniciando Proceso record...'));
	console.log(addToLog('    Consultando datos de table muestras en e-Register...'));
	eregister.query(queries.eregister.record
		, function(error, records, fields){
			if(error){
				console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
				writeLog(function(){cb(error);});
			}
			else{
				console.log(addToLog('    ...Se han obtenido '+records.length+' registros de e-Register'));
				console.log(addToLog('      Consultando propiedades en e-Register...'));
				eregister.query(queries.eregister.record_detail
				, function(error, properties, fields){
					if(error){
						console.log(addToLog('      ...No se han podido obtener datos de e-Register. '+error));
						writeLog(function(){cb(error);});
					}
					else{
						console.log(addToLog('      ...Se han obtenido '+properties.length+' propiedades de e-Register'));
						console.log(addToLog('      Relacionando registros con propiedades...'));
						writeLog();
						var items = [];
						for(var i = 0; i < records.length; i++){
							var record = records[i];
							var _properties = [];
							for(var j = 0; j < properties.length; j++){
								var property = properties[j];
								if(record.id == property.record){
									_properties.push({
										  property: property.property
										, value: property.value
										, creator: record.user
										, modified: record.created
										, modified: (new Date()).getTime()
										, modifier: 0
										, deleted: false
										, deleter: 0
									});
								}
							}
							addToLog('      ...Se han obtenido '+_properties.length+' propiedades de e-Register para el lote '+record.reference);
							items.push({
								id: record.id
								, product: record.product
								, reference: record.reference
								, analysis_date: record.analysis_date
								, elaboration_date: record.elaboration_date
								, due_date: record.due_date
								, reception_date: record.reception_date
								, properties: _properties
								, user: record.user
								, veredict: record.veredict
								, remission: record.remission
								, quantity: record.quantity
								, existing_quantity: record.existing_quantity
								, supplier: record.supplier
								, satisfies: record.satisfies == 1
								, active: record.active == 1
								, clause: ''
								, created: record.created
								, creator: record.user
								, modified: record.created
								, modifier: 0
								, deleted: false
								, deleter: 0
							});
						}
						console.log(addToLog('      ...Relacion finalizada'));
						writeLog();
						console.log(addToLog('    Insertando datos en Qualitrix...'));
						qxdb.collection('record').drop(function(error){
							qxdb.collection('record').insert(items, function(error, doc){
								if(error){
									console.log(addToLog('    ...Error insertando en tabla record. '+error));
									writeLog(function(){cb(error);});
								}else{
									console.log(addToLog('    ...Tabla record poblada exitosamente'));
									qxdb.collection('record').createIndex('product', {unique: false}, function(error){
					    				if(error){
								    		console.log(addToLog('    ...Error insertando en tabla record en Qualitrix. '+error));
								    		writeLog(function(){cb(error);});
								    	}else{
								    		console.log(addToLog('    ...Indices creados exitosamente'));
								    		console.log(addToLog('    Creando contadores en tabla record...'));
								    		eregister.query(queries.counters.record
							    			, function(error, results, fields){
							    				if(error){
						    						console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
													writeLog(function(){cb(error);});
							    				}else{
							    					qxdb.collection('counters').insert({
							    						name: 'record',
							    						seq: results[0].counter,
							    						date: (new Date()).getTime()
							    					}, function(error, doc){
							    						if(error){
							    							console.log(addToLog('    ...Error insertando en tabla record. '+error));
							    						}else{
							    							console.log(addToLog('    Proceso record en finalizado...'));
							    						}
							    						writeLog(function(){cb(error);});
							    					});
							    				}
							    			});
								    	}
						    		});
								}
							});
						});
					}
				});
			}
		}
	);
}

function populateQualitrixCertificate(qxdb, cb){
	console.log(addToLog('  Iniciando Proceso certificate...'));
	console.log(addToLog('    Consultando datos de table certificado en e-Register...'));
	eregister.query(queries.eregister.certificate
		, function(error, certificates, fields){
			if(error){
				console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
				writeLog(function(){cb(error);});
			}
			else{
				console.log(addToLog('    ...Se han obtenido '+certificates.length+' registros de e-Register'));
				console.log(addToLog('      Consultando propiedades de certificados en e-Register...'));
				eregister.query(queries.eregister.certificate_properties
				, function(error, properties, fields){
					if(error){
						console.log(addToLog('      ...No se han podido obtener datos de e-Register. '+error));
						writeLog(function(){cb(error);});
					}
					else{
						console.log(addToLog('      ...Se han obtenido '+properties.length+' propiedades de e-Register'));
						console.log(addToLog('      Consultando valores de certificados en e-Register...'));
						eregister.query(queries.eregister.certificate_values
						, function(error, values, fields){
							if(error){
								console.log(addToLog('      ...No se han podido obtener datos de e-Register'+ error));
								writeLog(function(){cb(error);});
							}
							else{
								console.log(addToLog('      ...Se han obtenido '+values.length+' valores de e-Register'));
								console.log(addToLog('      Relacionando certificados con propiedades...'));
								writeLog();
								var items = [];
								for(var i = 0; i < certificates.length; i++){
									var item = certificates[i];
									var _properties = [];
									for(var j = 0; j < properties.length; j++){
										var property = properties[j];
										if(item.id == property.certificate){
											_properties.push({
												  property: property.id
												, name: property.name.replace(/(\\r)|((\<+\/*(html|HTML|head|HEAD|body|BODY|font|FONT)+([ a-zA-Z=\\"0-9]*)+\>))|( {2,})/g,"").trim()
											});
										}
									}
									addToLog('      ...Se han obtenido '+_properties.length+' propiedades de e-Register para el certificado '+item.id);

									var _values = [];
									var _records = [];
									var record_reference = "";
									for(var j = 0; j < values.length; j++){
										var v = values[j];
										if(item.id == v.certificate){
											if(record_reference != v.record){
												if(_records.length>0){
													_values.push({
														record: record_reference,
														values: _records
												  	});	
												}
											  	_records = [];
											  	_records.push({property: v.property, value: v.value.trim()});
											  	record_reference = v.record;
											}else{
												_records.push({property: v.property, value: v.value.trim()});
											}
										}
									}
									_values.push({
										record: record_reference,
										values: _records
								  	});	
									addToLog('      ...Se han obtenido '+_values.length+' valores de e-Register para el certificado '+item.id);
									items.push({
										id: item.id
										, product: item.product
										, customer: item.customer
										, quantity: item.quantity
										, presentation: item.presentation
										, remission: item.remission
										, date: item.created
										, active: true
										, clause: item.clause.replace(/(\\r)|((\<+\/*(html|HTML|head|HEAD|body|BODY|font|FONT)+([ a-zA-Z=\\"0-9]*)+\>))|( {2,})/g,"").trim()
										, properties: _properties
								        , values: _values
										, creator: item.creator
										, created: item.created
										, modified: (new Date()).getTime()
										, modifier: 0
										, deleted: false
										, deleter: 0
									});
								}
								console.log(addToLog('      ...Relacion finalizada'));
								writeLog();
								console.log(addToLog('    Insertando datos en Qualitrix...'));
								qxdb.collection('certificate').drop(function(error){
									qxdb.collection('certificate').insert(items, function(error, doc){
										if(error){
											console.log(addToLog('    ...Error insertando en tabla certificate. '+error));
											writeLog(function(){cb(error);});
										}else{
											console.log(addToLog('    ...Tabla certificate poblada exitosamente'));
											qxdb.collection('certificate').createIndex('id', {unique: true}, function(error){
							    				if(error){
										    		console.log(addToLog('    ...Error insertando en tabla certificate en Qualitrix. '+error));
										    		writeLog(function(){cb(error);});
										    	}else{
										    		console.log(addToLog('    ...Indices creados exitosamente'));
										    		console.log(addToLog('    Creando contadores en tabla certificate...'));
										    		eregister.query(queries.counters.certificate
									    			, function(error, results, fields){
									    				if(error){
								    						console.log(addToLog('    ...No se han podido obtener datos de e-Register. '+error));
															writeLog(function(){cb(error);});
									    				}else{
									    					qxdb.collection('counters').insert({
									    						name: 'certificate',
									    						seq: results[0].counter,
									    						date: (new Date()).getTime()
									    					}, function(error, doc){
									    						if(error){
									    							console.log(addToLog('    ...Error insertando en tabla certificate. '+error));
									    						}else{
									    							console.log(addToLog('    Proceso certificate en finalizado...'));
									    						}
									    						writeLog(function(){cb(error);});
									    					});
									    				}
									    			});
										    	}
								    		});
										}
									});
								});
							}
						});
					}
				});
			}
		}
	);
}

console.log(addToLog("e-Register migrator to Qualitrix"));
startProcess();
