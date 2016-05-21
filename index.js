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

startProcess();

function startProcess(){
	console.log('Iniciando ejecución');
	openQualitrixConnection(function(db){
		openEregisterConnection(function(error){
			if(error){
				closeQualitrixConnection(db, endProcess);
			}else
			populateQualitrixToken(db, function(error){
				if(error){
					closeEregisterConnection(function(){
						closeQualitrixConnection(db, endProcess);
					});
				}else
				populateQualitrixSubsidiary(db, function(error){
					if(error){
						closeEregisterConnection(function(){
							closeQualitrixConnection(db, endProcess);
						});
					}else
					closeEregisterConnection(function(){
						closeQualitrixConnection(db, endProcess);
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
	qualitrix.open(function(err, db) {
		if(err){
			console.log('...No ha sido posible conectar Qualitrix, terminando ejecución');
			process.exit(1);
		}else{
			console.log('...Conectado con Qualitrix');
			cb(db);
		}
	});
}

function closeQualitrixConnection(db, cb){
	console.log('Desconectando de Qualitrix...');
	setTimeout(function(){
		db.close(function(error){
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

function populateQualitrixToken(db, cb){
	db.collection('subsidiary').drop(function(error){
		console.log('  Insertando en tabla token...');
		db.collection('token').insert({
			id: db.collection('token').count()
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

function populateQualitrixSubsidiary(db, cb){
	db.collection('subsidiary').drop(function(error){
		console.log('  Insertando en tabla subsidiary...');
		db.collection('subsidiary').insert({
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

function populateQualitrixProfile(db, cb){
	
	/*
	db.collection('profile').drop(function(error){
		console.log('  Insertando en tabla profile...');
		db.collection('profile').insert({
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
	    		console.log('  ...Error insertando en tabla profile');
	    		cb(error);
	    	}else{
	    		console.log('  ...Tabla profile poblada exitosamente');
	    		cb();
	    	}
	    });
	});
	*/
}