const rc = require('rc');
const commander = require('commander');
const _ = require('lodash');
const path = require('path');
const Sequelize = require('sequelize');
const winston = require('winston');
const async = require('async');
const Taleo = require('taleo-node-sdk');
const SpringCM = require('springcm-node-sdk');

require('winston-daily-rotate-file');

commander.version('0.1.0', '-v, --version');

// TODO: When/if args  for database, taleo remote, etc. are added, use those as rc defaults
var conf = rc('caas');

/**
 * Some objects we'll want easy access to:
 * - SpringCM SDK client
 * - Taleo SDK client
 * - SpringCM ADP lookup ref
 * - ORM
 */
var springCm, taleo, adpExtract, sequelize;
// Winston transports
var fileTransport, consoleTransport;
// Which location IDs to sync Taleo employees for
var validLocations;
// ORM models
var models = {};

async.waterfall([
  (callback) => {
    fileTransport = new (winston.transports.DailyRotateFile)({
      filename: path.join(__dirname, 'logs', 'log-%DATE%.log'),
      datePattern: 'YYYY-MM-DD',
      maxFiles: '14d'
    });

    consoleTransport = new (winston.transports.Console)();

    winston.configure({
      level: 'info',
      transports: [
        consoleTransport,
        fileTransport
      ]
    });

    winston.handleExceptions([ consoleTransport, fileTransport ]);

    winston.info('========================================');
    winston.info('taleo-springcm-sync');
    winston.info('========================================');

    callback();
  },
  (callback) => {
    var database = _.get(conf, [ 'taleo-springcm-sync', 'database', 'database' ]);
    var hostname = _.get(conf, [ 'taleo-springcm-sync', 'database', 'hostname' ]);
    var username = _.get(conf, [ 'taleo-springcm-sync', 'database', 'username' ]);
    var password = _.get(conf, [ 'taleo-springcm-sync', 'database', 'password' ]);

    winston.info('Connecting to', database, 'with username', username);

    seq = new Sequelize(database, username, password, {
      host: hostname,
      dialect: 'mssql',
      timezone: 'America/Los_Angeles',
      dialectOptions: {
        connectTimeout: 15000
      },
      retry: {
        max: 5
      },
      logging: false
    });

    seq.authenticate().then(() => {
      models.ActivityExport = seq.define('activity_export', {
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

      models.AttachmentExport = seq.define('attachment_export', {
        attachment: {
          type: Sequelize.INTEGER,
          primaryKey: true
        },
        page_count: Sequelize.INTEGER,
        attachment_description: Sequelize.STRING(400),
        attachment_type: Sequelize.STRING(40),
        employee_id: Sequelize.INTEGER,
        attachment_id: Sequelize.INTEGER,
        employee_name: Sequelize.STRING(400),
        exception_upload: Sequelize.INTEGER
      });
    }).then(() => {
      winston.info('Syncing with database');

      seq.sync().then(() => {
        sequelize = seq;
        callback();
      });
    }).catch(callback);
  },
  (callback) => {
    /**
     * Log in to SpringCM
     */

    winston.info('Connecting to SpringCM');

    client = new SpringCM(_.get(conf, 'taleo-springcm-sync.springCm.auth'));

    client.connect((err) => {
      if (err) {
        return callback(err);
      }

      springCm = client;

      callback();
    });
  },
  (callback) => {
    /**
     * Get ADP extract in SpringCM
     */

    winston.info('Getting ADP extract CSV in SpringCM');

    springCm.getDocument('/PMH/Alta Hospitals/Human Resources/_Admin/Employee Information.csv', (err, doc) => {
      if (err) {
        return callback(err);
      }

      adpExtract = doc;
      callback();
    });
  },
  (callback) => {
    /**
     * Log in to Taleo
     */

    winston.info('Connecting to Taleo');

    client = new Taleo(_.get(conf, 'taleo-springcm-sync.taleo.auth'));

    client.connect((err) => {
      if (err) {
        return callback(err);
      }

      taleo = client;

      callback();
    });
  },
  (callback) => {
    /**
     * Create a list of valid location IDs so we can filter employees to sync
     */

    validLocations = _.uniq(_.flatten(_.map(_.get(conf, 'taleo-springcm-sync.taleo.locations'), loc => loc.locationIds)));

    callback();
  },
  (callback) => {
    /**
     * Create a queue that will process employees one at a time, uploading
     * first any activities not already synchronized with SpringCM, then
     * uploading employee attachments.
     */

    var queue = async.queue((employee, callback) => {
      async.waterfall([
        (callback) => {
          /**
           * Verify the employee's information is in SpringCM, and is
           * able to route. Any Taleo employees with invalid or missing SSNs
           * are skipped.
           */

          var employeeSsn = employee.getSsn();

          // Skip if no SSN
          if (!employeeSsn) {
            return callback(`Missing SSN for Taleo employee ${employee.getId()}`);
          }

          // Filter out non-digits
          employeeSsn = employeeSsn.replace(/[^\d]*/g, '');

          // Skip if not a full-length SSN
          if (employeeSsn.length !== 9) {
            return callback(`Invalid SSN for Taleo employee ${employee.getId()}`);
          }

          // Do a lookup against the ADP extract in SpringCM for this employee
          // by SSN.
          springCm.csvLookup(adpExtract, {
            'Social Security Number': employeeSsn
          }, (err, rows) => {
            if (err) {
              return callback(err);
            }

            // We expect a single row to be returned
            if (rows.length === 0) {
              return callback(`Taleo employee ${employee.getId()} missing from ADP extract`);
            } else if (rows.length !== 1) {
              return callback(`Multiple rows in ADP extract for Taleo employee ${employee.getId()}`);
            }

            callback();
          });
        },
        (callback) => {
          /**
           * Get all packets for the employee, then retrieve all activities
           * for each packet. Upload activities to SpringCM.
           */

          taleo.getPackets(employee, (err, packets) => {
            if (err) {
              winston.error(err);
              return callback();
            }

            winston.info('Found', packets.length, 'packets for employee', employee.getId());

            // Get all activities for the packet
            async.eachSeries(packets, (packet, callback) => {
              taleo.getActivities(packet, (err, activities) => {
                winston.info('Found', activities.length, 'activities for packet', packet.getId());

                async.eachSeries(activities, (activity, callback) => {
                  callback();
                }, callback);
              });
            }, (err) => {
              if (err) {
                return callback(err);
              }

              callback();
            });
          });
        },
        (callback) => {
          /**
           * Get all attachments for the employee, then upload to SpringCM.
           */

          taleo.getAttachments(employee, (err, attachments) => {
            if (err) {
              return callback(err);
            }

            winston.info('Found', attachments.length, 'attachments for employee', employee.getId());

            async.eachSeries(attachments, (attachment, callback) => {
              callback();
            }, callback);
          });
        }
      ], (err) => {
        if (err) {
          winston.error(err);
        }

        callback();
      });
    });

    // Pass queue on; employees will be enqueued as they are retrieved
    callback(null, queue);
  },
  (queue, callback) => {
    /**
     * Begin retrieving employees from Taleo in pages, enqueueing up any
     * employee at a valid location as we receive them.
     */

    var start = 100;
    var length = 0;
    var max = 5;

    async.doUntil((callback) => {
      taleo.getEmployees({
        start: start,
        limit: 5
      }, (err, employees) => {
        if (err) {
          return callback(err);
        }

        length = employees.length;

        if (length === 0) {
          return callback();
        }

        if (length > 1) {
          winston.info('Queueing employees', start, 'through', start + length);
        } else if (length === 1) {
          winston.info('Queueing employee', start);
        }

        _.each(_.filter(employees, e => validLocations.indexOf(e.getLocation()) > -1), e => queue.push(e));

        start += length;

        callback();
      });
    }, () => {
      return length === 0 || start > max;
    }, (err) => {
      if (err) {
        return callback(err);
      }

      if (queue.length() > 0 || queue.running() > 0) {
        queue.drain = callback;
      } else {
        callback();
      }
    });
  }
], (err) => {
  if (err) {
    winston.error(err);
  }

  // Close/log out of Taleo and SpringCM and disconnect from database
  async.waterfall([
    (callback) => {
      if (springCm) {
        winston.info('Disconnecting from SpringCM');
        springCm.close(callback);
      } else {
        callback();
      }
    },
    (callback) => {
      if (taleo) {
        winston.info('Disconnecting from Taleo');
        taleo.close(callback);
      } else {
        callback();
      }
    },
    (callback) => {
      if (sequelize) {
        sequelize.close().then(() => {
          callback();
        }).catch(callback);
      } else {
        callback();
      }
    }
  ], (err) => {
    var code = 0;

    if (err) {
      winston.error(err);
      code = 1;
    }

    // Wait for our winston transports to finish streaming data, then exit
    async.parallel([
      callback => fileTransport.on('finished', callback),
      callback => consoleTransport.on('finished', callback)
    ], () => {
      process.exit(code);
    });
  });
});
