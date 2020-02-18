const csv = require('csv-parser');
const fs = require('fs');
const {bufferCount, concatMap, delay} = require('rxjs/operators');
const {from, of} = require('rxjs');

const {Client} = require('@elastic/elasticsearch')
const indexName = 'heroes';

async function run() {
    // Create Elasticsearch client
    const client = new Client({node: 'http://localhost:9200'});

    // Création de l'indice
    client.indices.create({index: indexName}, (err, resp) => {
        if (err) console.trace(err.message);
    });

    let heroes = [];
    let counter = 0;
    // Read CSV file
    fs.createReadStream('all-heroes.csv')
        .pipe(csv({
            separator: ','
        }))
        .on('data', (data) => {
            heroes.push({
                id: data["id"],
                name: data["name"],
                description: data["description"],
                secretIdentities: data["secretIdentities"],
                aliases: data["aliases"],
                partners: data["partners"],
                imageUrl: data["imageUrl"],
                gender: data["gender"],
                universe: data["universe"]
            });
        })
        .on('end', () => {
            from(heroes).pipe(
                bufferCount(200),
                concatMap(data => of(data).pipe(delay(1000)))
            ).subscribe(bufferedHeroes => {
                counter += bufferedHeroes.length;
                console.log(counter);
                client.bulk(createBulkInsertQuery(bufferedHeroes), (err, resp) => {
                    if (err) console.trace(err.message);
                    else console.log(`Inserted ${resp.body.items.length} heroes`);
                    // client.close();
                });
            });
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(heroes) {
    const body = heroes.reduce((acc, hero) => {
        let {secretIdentities, aliases, partners, ...rest} = hero;

        secretIdentities = split(secretIdentities);
        aliases = split(aliases);
        partners = split(partners);

        let name_suggest = [];
        name_suggest.push(formatInputWeight(rest.name, 10));
        secretIdentities.forEach(s => name_suggest.push(formatInputWeight(s, 5)));
        aliases.forEach(a => name_suggest.push(formatInputWeight(a, 5)));

        const heroFormatted = {
            ...rest,
            secretIdentities,
            aliases,
            partners,
            name_suggest
        };

        acc.push({index: {_index: indexName, _type: '_doc', _id: hero.id}});
        acc.push(heroFormatted);
        return acc
    }, []);

    return {body};
}

function split(str) {
    return str ? str.split(',') : [];
}

function formatInputWeight(input, weight) {
    return {
        input,
        weight
    }
}

run().catch(console.error);
