const Sequelize = require('sequelize');

// Wrapper for Sequelize init and model definitions
module.exports = {
	initialize: (host, db, username, password, callback) => {
		var sequelize = new Sequelize(db, username, password, {
			host: host,
			dialect: 'mssql',
			timezone: 'America/Los_Angeles',
			pool: {
				max: 5,
				min: 0,
				acquire: 30000,
				idle: 10000
			},
			logging: false
		});

		var models = {};

		sequelize.authenticate().then(() => {
			models.customer = sequelize.define('export', {
				activity: {
					type: Sequelize.INTEGER,
					primaryKey: true
				}
			});
		}).then(() => {
			sequelize.sync().then(() => {
				callback(null, sequelize);
			});
		}).catch((err) => {
			callback(err);
		});
	}
};
