const winston = require('winston');
const async = require('async');
// Require local library version, these other libraries aren't stable yet
const Taleo = require('taleo-node-sdk');
const SpringCM = require('springcm-node-sdk');
const dotenv = require('dotenv');

dotenv.config();

var springCm, taleo, adpExtract;

async.waterfall([
  (callback) => {
    /**
     * Log in to SpringCM
     */

    winston.info('Connecting to SpringCM');

    springCm = new SpringCM({
      dataCenter: 'uatna11',
      clientId: process.env.SPRINGCM_CLIENT_ID,
      clientSecret: process.env.SPRINGCM_CLIENT_SECRET
    });

    springCm.connect(callback);
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

    taleo = new Taleo({
      orgCode: process.env.TALEO_COMPANYCODE,
      username: process.env.TALEO_USERNAME,
      password: process.env.TALEO_PASSWORD
    });

    taleo.connect(callback);
  },
  (callback) => {
    winston.info('Compiling list of employees');

    taleo.getEmployees({
      start: 1,
      limit: 50
    }, (err, employees) => {
      if (err) {
        return callback(err);
      }

      winston.info(`Found ${employees.length} employees in Taleo`);

      callback(null, employees);
    });
  },
  (employees, callback) => {
    var queue = async.queue((employee, callback) => {
      async.waterfall([
        (callback) => {
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
          // Get all packets for the employee
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
              });
            }, (err) => {
              if (err) {
                return callback(err);
              }

              callback();
            });
          });
        }
      ], (err) => {
        if (err) {
          winston.error(err);
        }

        callback();
      });
    });

    queue.push(employees);
    queue.drain = callback;
  },
  // Get packets & activities for non-exception employees
  // Upload files, add data to load list for successful uploads
  // On upload, pull page count from SpringCM response
  // Upload load lists
], (err) => {
  if (err) {
    winston.error(err);
  }

  // Close/log out of Taleo and SpringCM
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
    }
  ], (err) => {
    if (err) {
      winston.error(err);
      process.exit(1);
    }

    process.exit(0);
  });
});
