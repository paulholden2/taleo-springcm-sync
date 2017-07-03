const fs = require('fs');
// Require local library version, these other libraries aren't stable yet
const Taleo = require('../taleo-nodejs-sdk');
const SpringCM = require('../springcm-nodejs-sdk');
const async = require('async');

var locations = {};

function getLocations(callback) {
	Taleo.location.all((err, locs) => {
		if (err) {
			return callbac(err);
		}

		locs.forEach((loc) => {
			locations[loc.id] = loc;
		});

		callback(null);
	});
}

function iterateEmployees(employees, callback) {
	async.eachSeries(employees, (emp, next) => {
		Taleo.employee.packets(emp, (err, packets) => {
			if (err) {
				console.log(err);
				return next(null);
			}

			console.log(`${emp.id} ${emp.firstName} ${emp.lastName}` + (emp.location ? ` - ${locations[emp.location].city}` : ' - Unknown Location'));

			SpringCM.folder.get(`/Taleo Packet Uploads/${emp.id} ${emp.firstName} ${emp.lastName}`, (err, fld) => {
				if (err) {
					console.log(err);
					return next(null);
				}

				async.eachSeries(packets, (packet, next) => {
					Taleo.packet.activities(packet, (err, activities) => {
						async.eachSeries(activities, (actv, next) => {
							var docname = `${actv.id} ${actv.title}.pdf`;

							if (!actv.signed()) {
								console.log(`Skipping ${actv.id}, not complete`);
								return next(null);
							}

							SpringCM.document.path(`/Taleo Packet Uploads/${emp.id} ${emp.firstName} ${emp.lastName}/${docname}`, (err, doc) => {
								if (!doc) {
									Taleo.activity.download(actv, `${__dirname}/${docname}`, (err) => {
										if (err) {
											console.log(err);
											next(null);
										} else {
											SpringCM.folder.upload(fld, `${__dirname}/${docname}`, null, (err) => {
												if (err) {
													console.log(err);
												} else {
													console.log(`Uploaded /Taleo Packet Uploads/${emp.id} ${emp.firstName} ${emp.lastName}/${docname}`);
													fs.unlinkSync(`${__dirname}/${docname}`);
												}

												next(null);
											});
										}
									});
								} else {
									console.log(`/Taleo Packet Uploads/${emp.id} ${emp.firstName} ${emp.lastName}/${docname} already exists`);
									next(null);
								}
							});
						}, (err) => {
							next(err);
						});
					});
				}, (err) => {
					next(err);
				});
			});
		});
	}, (err) => {
		if (err) {
			console.log(err);
		} else {
			callback(null);
		}
	});
}

function iteratePages(pages, callback) {
	async.eachSeries(pages, (page, next) => {
		page.read((err, employees) => {
			iterateEmployees(employees, (err) => {
				next(null);
			});
		});
	}, (err) => {
		if (err) {
			console.log(err);
		} else {
			callback(null);
		}
	});
}

async.waterfall([
	// SpringCM auth
	(callback) => {
		SpringCM.auth.uatna11(process.env.SPRINGCM_CLIENT_ID, process.env.SPRINGCM_CLIENT_SECRET, (err, token) => {
			callback(err);
		});
	},
	// Taleo dispatcher service
	(callback) => {
		Taleo.dispatcher.serviceURL((err, url) => {
			callback(err);
		});
	},
	(callback) => {
		getLocations(callback);
	},
	// Get employee pages
	(callback) => {
		Taleo.employee.pages(100, (err, pages) => {
			callback(err, pages);
		});
	},
	// Iterate through pages
	(pages, callback) => {
		iteratePages(pages, (err) => {
			if (err) {
				callback(err);
			} else {
				console.log('Completed employee page');
			}
		});
	}
], (err) => {
	if (err) {
		console.log(err);
		process.exit(1);
	} else {
		console.log('done');
		process.exit(0);
	}
});
