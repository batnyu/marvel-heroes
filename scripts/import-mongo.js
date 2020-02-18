var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");
const {bufferCount, concatMap, delay} = require('rxjs/operators');
const {from, of} = require('rxjs');

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";

// Script variables
const sourcePath = "all-heroes.csv";
const bufferSize = 200;
const delayBetweenImport = 500; // ms

let counter = 0;

async function run() {

    MongoClient.connect(
        mongoUrl,
        { useNewUrlParser: true, useUnifiedTopology: true },
        (err, client) => {
            if (err) throw err;
            let db = client.db(dbName).collection(collectionName);
            insertFromCsv(sourcePath, db);
        }
    );
}

function insertFromCsv(sourcePath, db){
    readCsv(sourcePath, (heroes) => insertData(heroes, db));
}

function readCsv(sourcePath, callback){
    let heroes = [];
    fs.createReadStream(sourcePath)
        .pipe(csv({
            separator: ";"
        }))
        .on("data", (data) => heroes.push(data))
        .on("end", () => {
            callback(heroes);
        });
}

function insertData(csvdata, db){
    from(csvdata).pipe(
        bufferCount(bufferSize),
        concatMap(data => of(data).pipe(delay(delayBetweenImport)))
    ).subscribe(bufferedData => {
        counter += bufferedData.length;
        console.log(counter);
        db.insertMany(bufferedData, (err, res) => {
            if (err) throw err;
            console.log(`Inserted: ${res.insertedCount} rows`);
        });
    });
}

run().catch(console.error);
