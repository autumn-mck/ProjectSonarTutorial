import { MongoClient } from "mongodb";

/**
 * Main function
 */
async function main() {
	// Database is currently hosted on same machine
	const uri = "mongodb://localhost:27017";
	const client = new MongoClient(uri);

	try {
		// Connect to the MongoDB cluster
		await client.connect();
	} catch (e) {
		// Log any errors
		console.error(e);
	} finally {
		await client.close();
	}
}

// Run the main function
main().catch(console.error);
