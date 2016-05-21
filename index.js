console.log("e-Register migrator to Qualitrix");

var mysql = require('mysql');
var mongodb = require('mongodb').MongoClient;

var eregister = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : '',
  database : 'eregister'
});

var qualitrix;

console.log('Conectando con Qualitrix...');
mongodb.connect('mongodb://localhost:27017/qx', function(error, db){
	if(error){
		console.log('No se ha podido conectar con Qualitrix');
		console.log('Cerrando Migración');
		process.exit(2);
	}else{
		qualitrix = db;
		console.log('Conectado con Qualitrix');
		createNewData();
	}
});

function createNewData(){
	console.log('Creando datos nuevos');
	
}

function migrateOldData(){
	
}

function eregisterConnect(){
	if(eregister){
		console.log('Conectando con e-Register...');
		eregister.connect();
		console.log('Conectado con e-Register');	
	}
}

function eregisterDisconnect(){
	if(eregister){
		console.log('Cerrando conexión con e-Register...');
		eregister.end();	
		console.log('Desconectado de e-Register');
	}
}

function qualitrixDisconnect(){
	if(qualitrix){
		console.log('Cerrando conexión con Qualitrix...');
		qualitrix.end();	
		console.log('Desconectado de Qualitrix');
	}
}




