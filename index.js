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
				populateQualitrixSubsidiary(qxdb, function(error){
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
						closeEregisterConnection(function(){
							closeQualitrixConnection(qxdb, endProcess);
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
	qxdb.collection('subsidiary').drop(function(error){
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
	        , deleted: 0
	        , deleter: 0
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

function populateQualitrixSubsidiary(qxdb, cb){
	qxdb.collection('subsidiary').drop(function(error){
		console.log('  Insertando en tabla subsidiary...');
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

function populateQualitrixProfile(qxdb, cb){
	console.log('  Insertando en tabla profile...');
	console.log('    Consultando datos de e-Register...');
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