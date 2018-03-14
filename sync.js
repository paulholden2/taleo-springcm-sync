const fs = require('fs');
const path = require('path');
const async = require('async');
const Taleo = require('taleo-nodejs-sdk');
const SpringCM = require('springcm-node-sdk');
const orm = require('./orm.js');
const PDFParser = require('pdf2json');

const tag = `[${process.pid}] `;

const dest = '/PMH/Alta Hospitals/Human Resources/Foothills/_Admin/Stria Deliveries/';

function plog(msg) {
	process.send({
		status: 'log',
		log: msg
	});
}

function iterateActivities(sequelize, employeeLookup, locationLookup, activities, callback) {
	async.eachSeries(activities, (actv, next) => {
		var emp = employeeLookup[actv.employeeID];
		var loc = emp.location && locationLookup[emp.location];
		var docname = `${emp.id} ${emp.firstName} ${emp.lastName} - ${actv.id} ${actv.title.replace(/[\\\/:<>"|*]/g, '_')}.pdf`;

		if (!actv.destinationPath) {
			plog(tag + 'No destination path set for activity ' + actv.id);
			return next();
		}

		var dest = path.join(actv.destinationPath, docname);

		SpringCM.document.path(dest, (err, doc) => {
			if (!doc) {
				Taleo.activity.download(actv, `${__dirname}/${docname}`, (err) => {
					if (err) {
						plog(`${tag} ${err.message}`);
						next();
					} else {
						var pdfParser = new PDFParser();

						pdfParser.on('pdfParser_dataError', (err) => {
							plog(`${tag} ${err.message}`);
							next();
						});

						pdfParser.on('pdfParser_dataReady', (data) => {
							SpringCM.folder.path(actv.destinationPath, (err, folder) => {
								SpringCM.folder.upload(folder, fs.createReadStream(`${__dirname}/${docname}`), docname, null, (err) => {
									if (err) {
										plog(`${tag} ${err.message}`);
										next(null);
									} else {
										var pages = data.formImage.Pages.length;

										plog(tag + `Uploaded: ${dest}, ${pages} pages`);
										fs.unlinkSync(`${__dirname}/${docname}`);
										sequelize.models.export.create({
											activity: actv.id,
											activity_title: actv.title,
											activity_id: actv.id,
											employee_id: emp.id,
											employee_name: `${emp.firstName} ${emp.lastName}`,
											page_count: pages,
											exception_upload: actv.isException
										}).catch(next).then(() => {
											plog(tag + `Logged ${actv.id} in export database`);
											next();
										});
									}
								});
							});
						});

						if (fs.existsSync(`${__dirname}/${docname}`)) {
							pdfParser.loadPDF(`${__dirname}/${docname}`);
						} else {
							plog(`${tag} Unable to locate activity ${docname}, likely an error occurred during download`);
							next();
						}
					}
				});
			} else {
				plog(tag + `Already exists: ${dest}`);
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

	plog(tag + `Received ${activities.length} activities, allowance of ${allowance}`);

	async.waterfall([
		// Taleo dispatcher service
		(callback) => {
			Taleo.dispatcher.serviceURL((err, url) => {
				callback(err);
			});
		},
		// SpringCM auth
		(callback) => {
			SpringCM.auth.login('uatna11', process.env.SPRINGCM_CLIENT_ID, process.env.SPRINGCM_CLIENT_SECRET, (err, token) => {
				callback(err);
			});
		},
		(callback) => {
			orm.initialize('caas-rds.cw0pqculnfgu.us-east-1.rds.amazonaws.com', 'taleo', 'taleo', 'RA3FrBb29n4PfRTDfW', (err, inst) => {
				if (err) {
					return callback(err);
				}

				callback(null, inst);
			});
		},
		// Split activities to sync into 1 chunk per allowance
		// Allowance is allocated such that the program never
		// attempts to allocate more than 20 tokens from Taleo
		(sequelize, callback) => {
			var len = Math.ceil(activities.length / allowance) + 1;

			async.times(allowance, (n, next) => {
				var from = n * len;
				var to = Math.min(from + len, activities.length);

				plog(tag + `Section ${from} - ${to - 1}`);

				iterateActivities(sequelize, employeeLookup, locationLookup, activities.slice(from, to), (err) => {
					next(err);
				});
			}, (err) => {
				callback(err);
			});
		}
	], (err) => {
		if (err) {
			plog(err);
		}

		process.send({
			pid: process.pid,
			status: 'complete'
		});

		process.exit(err ? 1 : 0);
	});
});
