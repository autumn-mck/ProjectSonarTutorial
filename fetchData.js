import { MongoClient } from "mongodb";
import { parse as tldParse } from "tldts-experimental";
import zlib from "zlib";
import fs from "fs";
import { get as getHttps } from "https";
import readline from "readline";

/**
 * Main function
 */
async function main() {
	// Database is currently hosted on same machine
	const uri = "mongodb://localhost:27017";
	const client = new MongoClient(uri);

	try {
		// Connect to MongoDB
		await client.connect();

		// Drop the collection containg Project Sonar data
		try {
			await dropCollection(client, "sonardata");
		} catch {}

		await client.db("test_db").collection("sonardata").createIndex({ domainWithoutSuffix: "text" });

		const dataUrl = "https://opendata.rapid7.com/sonar.fdns_v2/2022-01-28-1643328400-fdns_a.json.gz";
		readFromWeb(client, dataUrl);
	} catch (e) {
		console.error(e);
	}
}

async function parseSonar(client, readstream) {
	// Pipe the response into gunzip to decompress
	let gunzip = zlib.createGunzip();

	let lineReader = readline.createInterface({
		input: readstream.pipe(gunzip),
	});

	let arr = [];
	let count = 0;
	lineReader.on("line", (line) => {
		let lineJson = JSON.parse(line);
		let hostname = lineJson.name;
		if (hostname.substring(0, 2) === "*.") hostname = hostname.substring(2);

		let tldParsed = tldParse(hostname);

		if (tldParsed.domainWithoutSuffix) {
			count++;
			arr.push({
				domainWithoutSuffix: tldParsed.domainWithoutSuffix,
				publicSuffix: tldParsed.publicSuffix,
				subdomain: tldParsed.subdomain,
				type: lineJson.type,
				value: lineJson.value,
			});
			if (count % 100000 === 0) {
				console.log(`${count} lines parsed`);
				createManyListings(client, arr, "sonardata");
				arr = [];
			}
		}
	});
}

/**
 * Add the given JSON to the database
 * @param {MongoClient} client MongoClient with an open connection
 * @param {JSON[]} newListing The new data to be added
 * @param {string} collection Name of the collection to add the data to
 * @param {string} dbName Name of the database the collection is in
 */
async function createManyListings(client, newListing, collection, dbName = "test_db") {
	client.db(dbName).collection(collection).insertMany(newListing, { ordered: false });
}

async function readFromFile(client) {
	const sonarDataLocation = "fdns_a.json.gz";
	let stream = fs.createReadStream(sonarDataLocation);
	parseSonar(client, stream);
}

async function readFromWeb(client, url) {
	getHttps(url, function (res) {
		if (res.statusCode === 200) {
			parseSonar(client, res);
		} else if (res.statusCode === 301 || res.statusCode === 302) {
			// Recursively follow redirects, only a 200 will resolve.
			console.log(`Redirecting to: ${res.headers.location}`);
			readFromWeb(client, res.headers.location);
		} else {
			console.log(`Download request failed, response status: ${res.statusCode} ${res.statusMessage}`);
		}
	}).on("error", function (e) {
		console.error(e);
	});
}

async function dropCollection(client, collection, dbName = "test_db") {
	client.db(dbName).collection(collection).drop();
}

// Run the main function
main().catch(console.error);
