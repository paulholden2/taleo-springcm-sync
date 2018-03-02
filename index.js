const fs = require('fs');
const os = require('os');
const child = require('child_process');
const async = require('async');
// Require local library version, these other libraries aren't stable yet
const Taleo = require('taleo-nodejs-sdk');
const SpringCM = require('springcm-node-sdk');
const csvjson = require('csvjson');

var ssnLookup = {};
var locationLookup = {};
var employeeLookup = {};

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

const validLocations = [].concat.apply([], locationInfo.map(loc => loc.locationIds));

async.waterfall([
	(callback) => {
		console.log('Logging into SpringCM');

		SpringCM.auth.login('uatna11', process.env.SPRINGCM_CLIENT_ID, process.env.SPRINGCM_CLIENT_SECRET, (err, token) => {
			callback(err);
		});
	},
	(callback) => {
		console.log('Locating employee lookup');

		SpringCM.document.path('/PMH/Alta Hospitals/Human Resources/_Admin/Employee Information.csv', (err, doc) => {
			if (err) {
				return callback(err);
			}

			callback(null, doc);
		});
	},
	(doc, callback) => {
		console.log('Downloading employee lookup');

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
		getLocations(callback);
	},
	// Get employee pages
	(callback) => {
		Taleo.employee.pages(200, (err, pages) => {
			callback(err, pages);
		});
	},
	// Combine pages into a single list of employees
	(pages, callback) => {
		var employees = [];

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
	// Filter out employees not from Foothill (location id: 1)
	(employees, callback) => {
		callback(null, employees.filter((emp) => {
			return validLocations.indexOf(emp.location) > -1;
		}));
	},
	// Create employee lookup
	(employees, callback) => {
		employees.forEach((emp) => {
			employeeLookup[emp.id] = emp;
		});

		callback(null, employees);
	},
	// Get a list of activities for all packets for each employee
	(employees, callback) => {
		var out = fs.createWriteStream('./out.csv');

		// Map each employee to an array of activities
		async.mapLimit(employees, 18, (employee, callback) => {
			Taleo.employee.packets(employee, (err, packets) => {
				// List of signed activities for this employee
				var activities = [];

				async.eachSeries(packets, (packet, next) => {
					Taleo.packet.activities(packet, (err, res) => {
						// Filter out unsigned/incomplete activity forms
						res.forEach((actv) => {
							if (Taleo.activity.signed(actv)) {
								activities.push(actv);
							}

							if (ssnLookup.hasOwnProperty(employee.ssn.replace('-', ''))) {
								var emp = ssnLookup[employee.ssn.replace('-', '')];
								out.write(`"${employee.id} ${employee.firstName} ${employee.lastName} - ${actv.id} ${actv.title}.pdf","${emp['Clock Number']}","${emp['Last Name']}","${emp['First Name']}","${actv.title.replace(/^\d{2} - /, '')}"\r\n`);
							}
						});

						console.log(`${activities.length} form${activities.length === 1 ? '' : 's'} in ${packets.length} packet${packets.length === 1 ? '' : 's'} found for ${employee.id} - ${employee.firstName} ${employee.lastName}`);

						next(err);
					});
				}, (err) => {
					callback(err, activities);
				});
			});
		}, (err, lists) => {
			out.close();

			callback(err, lists);
		});
	},
	// Flatten the 2D array of activities
	(lists, callback) => {
		var activities = [];

		lists.forEach((list) => {
			// May be undefined if employee had no packets
			if (list) {
				list.forEach((item) => {
					activities = activities.concat(item);
				});
			}
		});

		console.log(`${activities.length} total activities to sync to SpringCM`);

		callback(null, activities);
	},
	// Assign child processes a portion of the activities to sync to SpringCM
	(activities, callback) => {
		// CPU count
		var cpus = os.cpus().length;
		// Pages per process. Splice at this index
		var per = Math.ceil(activities.length / cpus);

		console.log(`${cpus} CPUs`);
		console.log(`${activities.length} activities`);
		console.log(`${per} activities per process`);

		// Split up activities to upload amongst processes
		for (var i = 0; i < cpus; ++i) {
			// If all pages are assigned, stop
			if (i * per >= activities.length) {
				break;
			}

			var c = child.fork('./sync');
			var cpid = c.pid;
			// How many asynchronous tracks each process may use
			// 20 max Taleo tokens, reserve one
			var allowance = Math.floor(19 / cpus);

			var from = i * per;
			var to = Math.min((i + 1) * per - 1, activities.length - 1);

			console.log(`Spawned child process ${cpid}, assigning ` + (from === to ? `activity ${from}` : `activities ${from} - ${to} (${to - from + 1} total)`));
			c.send(JSON.stringify({
				allowance: allowance,
				employeeLookup: employeeLookup,
				locationLookup: locationLookup,
				activities: activities.slice(from, to + 1)
			}));
		}

		callback(null);
	}
], (err) => {
	// TODO: Upload log file
	if (err) {
		console.log(err);
		process.exit(1);
	}
});
