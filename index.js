const Taleo = require('taleo-nodejs-sdk');
const async = require('async');

Taleo.dispatcher.serviceURL((err, url) => {
	Taleo.employee.pages(100, (err, pages) => {
		if (err) {
			return console.log(err);
		}

		pages.forEach((page) => {
			page.read((err, employees) => {
				if (err) {
					return console.log(err);
				}

				async.eachSeries(employees, (emp, next) => {
					Taleo.employee.packets(emp, (err, packets) => {
						if (err) {
							return console.log(err);
						}

						console.log(`${emp.id} ${emp.firstName} ${emp.lastName}`);

						async.eachSeries(packets, (pkt, next) => {
							Taleo.packet.activities(pkt, (err, activities) => {
								if (err) {
									return console.log(err);
								}

								async.eachSeries(activities, (actv, next) => {
									var path = __dirname + '/' + actv.id + ' ' + actv.title + '.pdf';

									console.log('Downloading: ' + path);

									next(null);
									/*
									Taleo.activity.download(actv, path, (err) => {
										if (!err) {
											console.log('Complete');
										}

										next(err);
									});
									*/
								}, (err) => {
									next(err);
								});
							});
						}, (err) => {
							next(err);
						});
					});
				}, (err) => {
					if (err) {
						return console.log(err.message);
					}
				});
			});
		});
	});
});
