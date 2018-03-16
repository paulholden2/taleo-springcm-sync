const fs = require('fs');
const path = require('path');
const os = require('os');
const orm = require('./orm.js');
const child = require('child_process');
const moment = require('moment');
const async = require('async');
// Require local library version, these other libraries aren't stable yet
const Taleo = require('taleo-nodejs-sdk');
const SpringCM = require('springcm-node-sdk');
const csvjson = require('csvjson');
const sync = require('./sync.js');

var ssnLookup = {};
var locationLookup = {};
var employeeLookup = {};
var logname = moment().format('YYYY-MM-DD HH.mm.ss') + '.log';
var logfile = fs.createWriteStream(logname);
var validCounts = {};

function log(msg) {
	console.log(msg);
	logfile.write(msg + '\n');
}

function getLocationPath(id) {
	for (var i = 0; i < locationInfo.length; ++i) {
		if (locationInfo[i].locationIds.indexOf(id) > -1) {
			return `/PMH/Alta Hospitals/Human Resources/${locationInfo[i].name}/_Admin/Stria Deliveries`;
		}
	}

	return null;
}

function getLocationName(id) {
	for (var i = 0; i < locationInfo.length; ++i) {
		if (locationInfo[i].locationIds.indexOf(id) > -1) {
			return locationInfo[i].name;
		}
	}

	return null;
}

function getLocations(callback) {
	Taleo.location.all((err, locs) => {
		if (err) {
			return callback(err);
		}

		locs.forEach((loc) => {
			locationLookup[loc.id] = loc;
		});

		callback(null);
	});
}

const locationInfo = [
	{
		name: 'Alta Corp',
		locationIds: [ 39, 38, 70 ]
	},
	{
		name: 'Bellflower',
		locationIds: [ 37 ]
	},
	{
		name: 'Culver City',
		locationIds: [ 43 ]
	},
	{
		name: 'Foothills',
		locationIds: [ 1 ]
	},
	{
		name: 'Hollywood',
		locationIds: [ 41 ]
	},
	{
		name: 'Los Angeles',
		locationIds: [ 86, 76, 36 ]
	},
	{
		name: 'Norwalk',
		locationIds: [ 40, 69 ]
	},
	{
		name: 'Van Nuys',
		locationIds: [ 42 ]
	}
];

var lookupFiles = {};

const validLocations = [].concat.apply([], locationInfo.map(loc => loc.locationIds));
var seq;

locationInfo.forEach(function (info) {
	var loc = info.name;
	lookupFiles[loc] = fs.createWriteStream(path.join(__dirname, loc + '.csv'));
	validCounts[loc] = 0;
});

async.waterfall([
	(callback) => {
		orm.initialize('caas-rds.cw0pqculnfgu.us-east-1.rds.amazonaws.com', 'taleo', 'taleo', 'RA3FrBb29n4PfRTDfW', (err, inst) => {
			if (err) {
				return callback(err);
			}

			seq = inst;
			callback();
		});
	},
	(callback) => {
		log('Logging into SpringCM');

		SpringCM.auth.login('uatna11', process.env.SPRINGCM_CLIENT_ID, process.env.SPRINGCM_CLIENT_SECRET, (err, token) => {
			callback(err);
		});
	},
	(callback) => {
		log('Locating employee lookup');

		SpringCM.document.path('/PMH/Alta Hospitals/Human Resources/_Admin/Employee Information.csv', (err, doc) => {
			if (err) {
				return callback(err);
			}

			callback(null, doc);
		});
	},
	(doc, callback) => {
		log('Downloading employee lookup');

		var ws = fs.createWriteStream('./lookup.csv');

		SpringCM.document.download(doc, ws, (err, doc) => {
			callback(err);
		});
	},
	(callback) => {
		var adp = csvjson.toObject(fs.readFileSync('./lookup.csv').toString(), {
			delimiter: ',',
			quote: '"'
		});

		callback(null, adp);
	},
	(adp, callback) => {
		adp.forEach((item) => {
			ssnLookup[item['Social Security Number']] = item;
		});

		callback();
	},
	// Taleo dispatcher service
	(callback) => {
		Taleo.dispatcher.serviceURL((err, url) => {
			callback(err);
		});
	},
	// Get locations (also creates location lookup)
	(callback) => {
		log('Getting locations in Taleo to build lookup');

		getLocations(callback);
	},
	// Get employee pages
	(callback) => {
		const n = 50;

		log('Pulling employee list in pages of ' + n);

		Taleo.employee.pages(n, (err, pages) => {
			callback(err, pages.slice(-3));
		});
	},
	// Combine pages into a single list of employees
	(pages, callback) => {
		var employees = [];

		log(`Compiling list of employees from ${pages.length} pages`);

		// Compile a list of all employees
		async.eachSeries(pages, (page, next) => {
			Taleo.page.read(page, (err, res) => {
				employees = employees.concat(res);
				next(err);
			});
		}, (err) => {
			callback(err, employees);
		});
	},
	// Filter out unwanted (not in that way) employees
	(employees, callback) => {
		log(`Filtering in employees at designated locations from a total of ${employees.length}`);

		callback(null, employees.filter((emp) => {
			return validLocations.indexOf(emp.location) > -1;
		}));
	},
	// Create employee lookup
	(employees, callback) => {
		log(`Generating employee ID -> employee lookup for ${employees.length} employees`);

		employees.forEach((emp) => {
			employeeLookup[emp.id] = emp;
		});

		callback(null, employees);
	},
	// Get a list of activities for all packets for each employee
	(employees, callback) => {
		log('Compiling list of employee activities');

		// Map each employee to an array of activities
		async.mapLimit(employees, 18, (employee, callback) => {
			Taleo.employee.packets(employee, (err, packets) => {
				// List of signed activities for this employee
				var activities = [];
				var exceptions = [];

				async.eachSeries(packets, (packet, next) => {
					Taleo.packet.activities(packet, (err, res) => {
						// Filter out unsigned/incomplete activity forms
						res.forEach((actv) => {
							if (actv.href.download) {
								if (ssnLookup.hasOwnProperty(employee.ssn.replace('-', ''))) {
									var emp = ssnLookup[employee.ssn.replace('-', '')];
									var loc = getLocationPath(employee.location);
									var locName = getLocationName(employee.location);

									actv.destinationPath = loc;
									actv.isException = 0;
									activities.push(actv);

									validCounts[locName] += 1;

									if (locName === 'Culver City') {
										lookupFiles[locName].write(`"${employee.id} ${employee.firstName} ${employee.lastName} - ${actv.id} ${actv.title.replace(/[\\\/]/g, '_')}.pdf","${emp['EMP ID']}","${emp['Last Name']}","${emp['First Name']}","${actv.title.replace(/^\d{2} - /, '').replace(/[\\\/]/g, '_')}",""\r\n`);
									} else {
										lookupFiles[locName].write(`"${employee.id} ${employee.firstName} ${employee.lastName} - ${actv.id} ${actv.title.replace(/[\\\/]/g, '_')}.pdf","${emp['EMP ID']}","${emp['Last Name']}","${emp['First Name']}","${actv.title.replace(/^\d{2} - /, '').replace(/[\\\/]/g, '_')}"\r\n`);
									}
								} else {
									actv.destinationPath = '/PMH/Alta Hospitals/Human Resources/_Admin/Taleo Sync/Exceptions';
									actv.isException = 1;
									exceptions.push(actv);
								}
							}
						});

						log(`${activities.length} form${activities.length === 1 ? '' : 's'} in ${packets.length} packet${packets.length === 1 ? '' : 's'} found for ${employee.id} - ${employee.firstName} ${employee.lastName}`);

						next(err);
					});
				}, (err) => {
					callback(err, {
						activities: activities,
						exceptions: exceptions
					});
				});
			});
		}, (err, lists) => {
			callback(err, lists);
		});
	},
	// Flatten the 2D array of activities
	(lists, callback) => {
		var activities = [];
		var exceptions = [];

		lists.forEach((list) => {
			// May be undefined if employee had no packets
			if (list) {
				list.activities.forEach((item) => {
					activities = activities.concat(item);
				});

				list.exceptions.forEach((item) => {
					exceptions = exceptions.concat(item);
				});
			}
		});

		log(`${activities.length} activities found in Taleo`);
		log(`${exceptions.length} exceptions`);
		log(`${exceptions.length + activities.length} total`);

		callback(null, activities.concat(exceptions));
	},
	(activities, callback) => {
		const lim = 10;

		async.filterLimit(activities, lim, (activity, callback) => {
			seq.models.export.findOne({
				where: {
					'activity': activity.id
				}
			}).catch(callback).then((row) => {
				if (row) {
					callback(null, false);
				} else {
					callback(null, true);
				}
			});
		}, (err, results) => {
			if (err) {
				return callback(err);
			}

			log(`${results.length} total activities to sync to SpringCM`);

			// Pass results to load activities into SpringCM
			// Pass empty array for testing
			callback(null, results);
		});
	},
	// Assign child processes a portion of the activities to sync to SpringCM
	(activities, callback) => {
		if (activities.length === 0) {
			log('No activities to upload');
			return callback();
		}

		sync(locationLookup, employeeLookup, activities, 18, seq, log, callback);
	},
	(callback) => {
		Object.keys(lookupFiles).forEach((key) => {
			lookupFiles[key].end();
		});

		callback();
	},
	(callback) => {
		// Upload load files
		async.eachSeries(locationInfo, (info, callback) => {
			if (validCounts[info.name] < 1) {
				log('Skipping upload of ' + info.name + '.csv; no files uploaded');
				return callback();
			}

			SpringCM.folder.path(`/PMH/Alta Hospitals/Human Resources/${info.name}/_Admin/Stria Deliveries`, (err, folder) => {
				if (err) {
					return callback(err);
				}

				SpringCM.folder.upload(folder, fs.createReadStream(path.join(__dirname, info.name + '.csv')), info.name + '.csv', null, (err) => {
					if (err) {
						log('Error while uploading load file ' + info.name + '.csv (might just be empty): ' + err);
					} else {
						log('Uploaded load file ' + info.name + '.csv');
					}

					fs.unlinkSync(path.join(__dirname, info.name + '.csv'));
					callback();
				});
			});
		}, (err) => {
			if (err) {
				return callback(err);
			}

			callback();
		});
	}
], (err) => {
	if (err) {
		log(err);
	}

	logfile.end();

	SpringCM.folder.path('/PMH/Alta Hospitals/Human Resources/_Admin/Taleo Sync/Logs', (err, folder) => {
		if (err) {
			return console.log(err);
		}

		SpringCM.folder.upload(folder, fs.createReadStream(logname), logname, null, (err) => {
			if (err) {
				return console.log(err);
			}

			fs.unlinkSync(logname);
			process.exit(err ? 1 : 0);
		});
	});
});
