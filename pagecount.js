const fs = require('fs');
const async = require('async');
const csvjson = require('csvjson');
const SpringCM = require('../springcm-node-sdk');

require('dotenv').config();

async.waterfall([
	(callback) => {
		SpringCM.auth.login('na11', process.env.SPRINGCM_CLIENT_ID, process.env.SPRINGCM_CLIENT_SECRET, (err, token) => {
			callback(err);
		});
	},
	(callback) => {
		var uids = csvjson.toObject(fs.readFileSync('./Taleo Uploads.csv').toString(), {
			delimiter: ',',
			quote: '"'
		});

		callback(null, uids.map(row => row['Uid']));
	},
	(uids, callback) => {
		var pages = 0;
		var docs = 0;

		async.eachLimit(uids, 15, (uid, callback) => {
			SpringCM.document.uid(uid, (err, doc) => {
				if (err) {
					return callback(err);
				}

				docs += 1;
				pages += doc.pages;
				console.log(`#${docs}: ${doc.pages} in ${uid}`)

				callback();
			});
		}, (err) => {
			if (err) {
				callback(err);
			}

			callback(null, docs, pages);
		});
	}
], (err, docs, pages) => {
	if (err) {
		console.log(err);
	}

	console.log(`docs: ${docs} pages: ${pages}`);
});
