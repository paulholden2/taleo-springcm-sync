const fs = require('fs');
const async = require('async');
const Taleo = require('taleo-nodejs-sdk');
const SpringCM = require('springcm-nodejs-sdk');

const tag = `[${process.pid}] `;

function iterateActivities(employeeLookup, locationLookup, folder, activities, callback) {
	async.eachSeries(activities, (actv, next) => {
		var emp = employeeLookup[actv.employeeID];
		var loc = emp.location && locationLookup[emp.location];
		var docname = `${emp.id} ${emp.firstName} ${emp.lastName} - ${actv.id} ${actv.title}.pdf`;

		SpringCM.document.path(`/Taleo Packet Uploads/${docname}`, (err, doc) => {
			if (!doc) {
				Taleo.activity.download(actv, `${__dirname}/${docname}`, (err) => {
					if (err) {
						console.log(tag + err);
						next(null);
					} else {
						SpringCM.folder.upload(folder, `${__dirname}/${docname}`, null, (err) => {
							if (err) {
								console.log(tag + err);
							} else {
								console.log(tag + `Uploaded: /Taleo Packet Uploads/${docname}`);
								fs.unlinkSync(`${__dirname}/${docname}`);
							}

							next(null);
						});
					}
				});
			} else {
				console.log(tag + `Already exists: /Taleo Packet Uploads/${docname}`);
				next(null);
			}
		});
	}, (err) => {
		callback(err);
	});
}

process.on('message', (msg) => {
	msg = JSON.parse(msg);

	var locationLookup = msg.locationLookup;
	var employeeLookup = msg.employeeLookup;
	var activities = msg.activities;
	var allowance = msg.allowance;
	var folder = null;

	console.log(tag + `Received ${activities.length} activities, allowance of ${allowance}`);

	async.waterfall([
		// Taleo dispatcher service
		(callback) => {
			Taleo.dispatcher.serviceURL((err, url) => {
				callback(err);
			});
		},
		// SpringCM auth
		(callback) => {
			SpringCM.auth.uatna11(process.env.SPRINGCM_CLIENT_ID, process.env.SPRINGCM_CLIENT_SECRET, (err, token) => {
				callback(err);
			});
		},
		(callback) => {
			SpringCM.folder.get('/Taleo Packet Uploads', (err, fld) => {
				folder = fld;
				callback(err);
			});
		},
		// Split activities to sync into 1 chunk per allowance
		// Allowance is allocated such that the program never
		// attempts to allocate more than 20 tokens from Taleo
		(callback) => {
			var len = Math.ceil(activities.length / allowance) + 1;

			async.times(allowance, (n, next) => {
				var from = n * len;
				var to = Math.min(from + len, activities.length);

				console.log(tag + `Section ${from} - ${to - 1}`);

				iterateActivities(employeeLookup, locationLookup, folder, activities.slice(from, to), (err) => {
					next(err);
				});
			}, (err) => {
				callback(err);
			});
		}
	], (err) => {
		process.send(err);
		process.exit(err ? 1 : 0);
	});
});
