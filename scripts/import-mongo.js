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

function run() {

    MongoClient.connect(
        mongoUrl,
        { useNewUrlParser: true, useUnifiedTopology: true },
        async (err, client) =>  {

            if (err) throw err;
            let db = client.db(dbName).collection(collectionName);

            db.deleteMany({}, function(err, numberOfRemovedDocs) {
                if(err) throw err;
                console.log("Dropped " + numberOfRemovedDocs + " documents")
            });

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
            separator: ","
        }))
        .on("data", (data) => heroes.push(parseHero(data)))
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

function toArray(string) {
    return string && (typeof string === "string") ? string.split(',') : [];
}

function toNumber(string) {
    return string && (typeof string === "string") ? parseInt(string) : null;
}

function toFloat(string) {
    return string && (typeof string === "string") ? parseFloat(string) : null;
}

function parseHero(data){
    return {
        id : data["id"],
        name : data["name"],
        imageUrl : data["imageUrl"],
        backgroundImageUrl : data["backgroundImageUrl"],
        externalLink : data["externalLink"],
        description : data["description"],
        identity : parseIdentity(data),
        appearance : parseAppearance(data),
        teams : toArray(data["teams"]),
        powers : toArray(data["powers"]),
        partners : toArray(data["partners"]),
        skills : parseSkills(data),
        creators : toArray(data["creators"])
    }
}

function parseIdentity(data){
    return {
        secretIdentities : toArray(data["secretIdentities"]),
        birthPlace : data["birthPlace"],
        occupation : data["occupation"],
        aliases : toArray(data["aliases"]),
        alignment : data["alignment"],
        firstAppearance : data["firstAppearance"],
        yearAppearance : toNumber(data["yearAppearance"]),
        universe : data["universe"]
    }
}

function parseAppearance(data){
    return {
        gender : data["gender"],
        type : data["type"],
        race : data["race"],
        height : toFloat(data["height"]),
        weight : toFloat(data["weight"]),
        eyeColor : data["eyeColor"],
        hairColor : data["hairColor"]
    }
}

function parseSkills(data){
    return {
        intelligence : toNumber(data["intelligence"]),
        strength : toNumber(data["strength"]),
        speed : toNumber(data["speed"]),
        durability : toNumber(data["durability"]),
        combat : toNumber(data["combat"]),
        power : toNumber(data["power"])
    }
}

run();
