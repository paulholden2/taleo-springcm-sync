const Sequelize = require('sequelize');

// Wrapper for Sequelize init and model definitions
module.exports = {
	initialize: (host, db, username, password, callback) => {
		var sequelize = new Sequelize(db, username, password, {
			host: host,
			dialect: 'mssql',
			timezone: 'America/Los_Angeles',
			pool: {
				max: 20,
				min: 1,
				acquire: 30000,
				idle: 30000
			},
			logging: false
		});

		var models = {};

		sequelize.authenticate().then(() => {
			models.customer = sequelize.define('export', {
				activity: {
					type: Sequelize.INTEGER,
					primaryKey: true
				},
				page_count: Sequelize.INTEGER,
				activity_title: Sequelize.STRING(400),
				employee_id: Sequelize.INTEGER,
				activity_id: Sequelize.INTEGER,
				employee_name: Sequelize.STRING(400),
				exception_upload: Sequelize.INTEGER
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
