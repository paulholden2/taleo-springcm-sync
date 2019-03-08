const fs = require('fs');
const rc = require('rc');
const commander = require('commander');
const _ = require('lodash');
const path = require('path');
const tmp = require('tmp');
const Sequelize = require('sequelize');
const winston = require('winston');
const async = require('async');
const Taleo = require('taleo-node-sdk');
const SpringCM = require('springcm-node-sdk');
const WinstonCloudWatch = require('winston-cloudwatch');
const department = require('./departments.js');
const status = require('./status.js');
const csvjson = require('csvjson');

require('winston-daily-rotate-file');

commander.version('0.1.0', '-v, --version');

// TODO: When/if args  for database, taleo remote, etc. are added, use those as rc defaults
var conf = rc('caaspms');

function documentNameFor(name) {
  switch (name) {
  case '01 PMS Arbitration Agreement':
    return 'Arbitration Agreement';
  case '02 PMS Driving Policy':
    return 'Driving Policy';
  case '03 - I-9':
    return 'I9';
  case '03 PMS Telephone Policy':
    return 'Company Issued Mobile Phone Policy';
  case '04 PMS IIPP':
    return 'IIPP';
  case '05 - W-4':
    return 'Tax Forms';
  case 'Arbitration Agreement PMCA':
    return 'Arbitration Agreement';
  case 'At-Will Acknowledgment':
    return 'At-Will Employment';
  case 'At-Will Acknowledgment PMCA':
    return 'At-Will Employment';
  case 'Code of Bus. Cond. and Ethical Bus. Ack. Form':
    return 'Code of Conduct';
  case 'Criminal Sanctions Exclusion Attestation PMCA':
    return 'Background Checks';
  case 'Discrimination & Harassment CA –PMCA (PMS Clinics)':
    return 'Unlawful Harassment';
  case 'Discrimination and Harassment Policy - PMH PMS CRC':
    return 'Unlawful Harassment';
  case 'Driving Policy PMCA':
    return 'Driving Policy';
  case 'Employee Information Sheet':
    return 'EE Info Sheet';
  case 'FAMA Acknowledgement - PMS/PMCA':
    return 'FAMA';
  case 'Meal Period Waiver Agreement PMS_PMCA':
    return 'Meal Period Waiver';
  case 'MSO/IPA Compliance Program':
    return 'MSO - IPA';
  case 'PMCA - Employee Handbook Acknowledgement':
    return 'Employee Handbook';
  case 'PMCA - HIPAA CONFIDENTIALITY AGREEMENT (PHI)':
    return 'HIPAA Agreement';
  case 'PMCA Vehicle Registration Form':
    return 'Vehicle Registration Form';
  case 'PMH Confidentiality Agreement':
    return 'Confidential Archive';
  case 'PMH EE_Handbook_Acknowledgement':
    return 'Employee Handbook';
  case 'PMS - Employee Handbook Acknowledgement':
    return 'Employee Handbook';
  case 'PMS HIPAA CONFIDENTIALITY AGREEMENT (PHI)':
    return 'HIPAA Agreement';
  case 'PMS IIPP Acknowledgement':
    return 'IIPP';
  case 'PMS Onboarding Criminal Sanctions Exclusion Attest':
    return 'Background Checks';
  case 'PMS Vehicle Registration Form':
    return 'Vehicle Registration Form';
  case 'Prospect and Subsidaries Confidentiality Agreement':
    return 'Confidential Archive';
  case 'Prospect Confidentiality Agreement':
    return 'Confidential Archive';
  case 'Telephone Call Recording':
    return 'Call Monitoring';
  default:
    return 'Other Docs';
  }
}

function subcategoryFor(name) {
  switch (name) {
  case '1040 Form':
  case 'Benefit Files':
  case 'Benefit Files Archive':
  case 'Benefits SCF':
  case 'Birth Certificate':
  case 'Enrollment':
  case 'Family Status Change':
  case 'Marriage License':
  case 'New Hire Letter':
  case 'No Change - Decline':
  case 'Tobacco Declaration':
  case 'Unum Cancellation':
    return 'Benefit Files';
  case 'EEOC':
  case 'EEOC Archive':
    return 'EEOC';
  case 'Code of Conduct':
  case 'Compliance Archive':
  case 'MSO - IPA ':
  case 'OIG - EPLS \'SAM\'':
  case 'Training Acknowledgment':
    return 'Compliance';
  case 'Background Checks':
  case 'Certs and Licenses':
  case 'Confidential Archive':
  case 'Employment Verification':
  case 'Misc. Confidential':
    return 'Confidential';
  case 'Arbitration Agreement':
  case 'At-Will Employment':
  case 'Benefit Acknowledgment ':
  case 'Call Monitoring':
  case 'COD':
  case 'Company Issued Mobile Phone Policy':
  case 'Driving Policy':
  case 'EE Info Sheet':
  case 'Employee Confidentiality Agreement':
  case 'Employee Handbook':
  case 'Ergo Evaluation':
  case 'HIPAA Agreement':
  case 'IIPP':
  case 'NDA Agreement':
  case 'New Hire Checklist':
  case 'Notice to Employee':
  case 'Onboarding Archive':
  case 'Prospect Medical Confidentiality Agreement':
  case 'SYOD':
  case 'Unlawful Harassment':
  case 'Vehicle Registration Form':
    return 'Onboarding';
  case 'Internal Application':
  case 'Other Archive':
  case 'Other Docs':
  case 'Safety Checklist':
  case 'Teleworker Agreement':
    return 'Other';
  case 'Direct Deposit Forms':
  case 'Tax Forms':
    return 'Payroll';
  case 'Counseling Forms':
  case 'Performance Archive':
  case 'Reviews and Evals':
    return 'Performance';
  case 'Application':
  case 'Interviews and References':
  case 'Job Description':
  case 'Offer Letter':
  case 'Pre-Employment Archive':
  case 'References':
  case 'Resume':
  case 'Staffing Requisition ':
  case 'Testing Results':
    return 'Pre-Employment';
  case 'SCF':
  case 'Status Changes Archive':
    return 'Status Changes';
  case 'Term File':
    return 'Term File';
  case 'I9':
  case 'I9 Archive':
    return 'I9';
  case 'ADA Letter':
  case 'Doctor Notes':
  case 'Exhausted FMLA':
  case 'Extension':
  case 'Initial Letter':
  case 'Leave of Absence':
  case 'Leave of Absence Archive':
  case 'Medical Cert':
  case 'Request':
    return 'Leave of absence';
  case 'Payroll':
  case 'Payroll Archive':
    return 'Payroll';
  case 'Workers Compensation':
  case 'Workers Compensation Archive':
    return 'Workers Compensation';
  default:
    return 'Other';
  }
}

function categoryFor(name) {
  switch (name) {
  case '1040 Form':
  case 'Benefit Files':
  case 'Benefit Files Archive':
  case 'Benefits SCF':
  case 'Birth Certificate':
  case 'Enrollment':
  case 'Family Status Change':
  case 'Marriage License':
  case 'New Hire Letter':
  case 'No Change - Decline':
  case 'Tobacco Declaration':
  case 'Unum Cancellation':
    return 'Benefit Files';
  case 'EEOC':
  case 'EEOC Archive':
    return 'EEOC';
  case 'Code of Conduct':
  case 'Compliance Archive':
  case 'MSO - IPA ':
  case 'OIG - EPLS \'SAM\'':
  case 'Training Acknowledgment':
    return 'Employee Files';
  case 'Background Checks':
  case 'Certs and Licenses':
  case 'Confidential Archive':
  case 'Employment Verification':
  case 'Misc. Confidential':
    return 'Employee Files';
  case 'Arbitration Agreement':
  case 'At-Will Employment':
  case 'Benefit Acknowledgment ':
  case 'Call Monitoring':
  case 'COD':
  case 'Company Issued Mobile Phone Policy':
  case 'Driving Policy':
  case 'EE Info Sheet':
  case 'Employee Confidentiality Agreement':
  case 'Employee Handbook':
  case 'Ergo Evaluation':
  case 'HIPAA Agreement':
  case 'IIPP':
  case 'NDA Agreement':
  case 'New Hire Checklist':
  case 'Notice to Employee':
  case 'Onboarding Archive':
  case 'Prospect Medical Confidentiality Agreement':
  case 'SYOD':
  case 'Unlawful Harassment':
  case 'Vehicle Registration Form':
    return 'Employee Files';
  case 'Internal Application':
  case 'Other Archive':
  case 'Other Docs':
  case 'Safety Checklist':
  case 'Teleworker Agreement':
    return 'Employee Files';
  case 'Direct Deposit Forms':
  case 'Tax Forms':
    return 'Employee Files';
  case 'Counseling Forms':
  case 'Performance Archive':
  case 'Reviews and Evals':
    return 'Employee Files';
  case 'Application':
  case 'Interviews and References':
  case 'Job Description':
  case 'Offer Letter':
  case 'Pre-Employment Archive':
  case 'References':
  case 'Resume':
  case 'Staffing Requisition ':
  case 'Testing Results':
    return 'Employee Files';
  case 'SCF':
  case 'Status Changes Archive':
    return 'Employee Files';
  case 'Term File':
    return 'Employee Files';
  case 'I9':
  case 'I9 Archive':
    return 'I9';
  case 'ADA Letter':
  case 'Doctor Notes':
  case 'Exhausted FMLA':
  case 'Extension':
  case 'Initial Letter':
  case 'Leave of Absence':
  case 'Leave of Absence Archive':
  case 'Medical Cert':
  case 'Request':
    return 'Leave of Absence';
  case 'Payroll':
  case 'Payroll Archive':
    return 'Payroll';
  case 'Workers Compensation':
  case 'Workers Compensation Archive':
    return 'Workers Compensation';
  default:
    return 'Employee Files';
  }
}


/**
 * Some objects we'll want easy access to:
 * - SpringCM SDK client
 * - Taleo SDK client
 * - SpringCM ADP lookup ref
 * - ORM
 */
var springCm, taleo, adpExtract, sequelize;
// Winston transports
var fileTransport, consoleTransport, cwlTransport;
// Which location IDs to sync Taleo employees for
var validLocations;
// ORM models
var models = {};
// Data of employee exceptions
var exceptionData = [];

async.waterfall([
  (callback) => {
    fileTransport = new (winston.transports.DailyRotateFile)({
      level: 'info',
      format: winston.format.simple(),
      filename: path.join(__dirname, 'logs', 'log-%DATE%.log'),
      datePattern: 'YYYY-MM-DD',
      maxFiles: '14d'
    });

    consoleTransport = new (winston.transports.Console)({
      level: 'info',
      format: winston.format.simple()
    });

    var transports = [
      consoleTransport,
      fileTransport
    ];

    var cw = _.get(conf, 'taleo-springcm-sync.logs.cloudwatch');

    if (cw) {
      // Set up logging to AWS CloudWatch Logs
      cwlTransport = new WinstonCloudWatch(_.merge(cw, {
        messageFormatter: (entry) => {
          return JSON.stringify(_.get(entry, 'meta'));
        }
      }));

      transports.push(cwlTransport);
    }

    winston.configure({
      transports: transports
    });

    winston.handleExceptions(transports);

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

    winston.info(`Connecting to ${hostname} with username ${username}`, {
      database: database,
      username: username,
      hostname: hostname
    });

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
      winston.info('Syncing with database', {
        database: database
      });

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

    var auth = _.get(conf, 'taleo-springcm-sync.springCm.auth');

    winston.info('Connecting to SpringCM', {
      clientId: auth.clientId,
      dataCenter: auth.dataCenter
    });

    client = new SpringCM(auth);

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

    var adpExtractPath = '/HR/_Admin - HR/Prospect Employee List.csv';

    winston.info('Getting ADP extract CSV in SpringCM', {
      path: adpExtractPath
    });

    springCm.getDocument(adpExtractPath, (err, doc) => {
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

    var auth = _.get(conf, 'taleo-springcm-sync.taleo.auth');

    winston.info('Connecting to Taleo', {
      orgCode: auth.orgCode,
      username: auth.username
    });

    client = new Taleo(auth);

    client.connect((err) => {
      if (err) {
        return callback(err);
      }

      taleo = client;

      winston.info('Received auth_token from Taleo', {
        authToken: taleo.authToken
      });

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
      const employeeName = `${employee.getFirstName()} ${employee.getLastName()}`;

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
            exceptionData.push({
              'SSN Status': 'Missing',
              'HRIS Extract Status': 'N/A',
              'Department ID': employee.getDepartment(),
              'Department Code': department.codeFor(employee.getDepartment()),
              'Department Name': department.nameFor(employee.getDepartment()),
              'Status': status.nameFor(employee.obj.employee.status),
              'Employee ID': employee.getId(),
              'Employee Last Name': employee.getLastName(),
              'Employee First Name': employee.getFirstName(),
              'Employee Creation Date': employee.obj.employee.creationDate
            });
            return callback(new Error(`Missing SSN for Taleo employee ${employee.getId()}`));
          }

          // Filter out non-digits
          employeeSsn = employeeSsn.replace(/[^\d]*/g, '');

          // Skip if not a full-length SSN
          if (employeeSsn.length !== 9) {
            exceptionData.push({
              'SSN Status': 'Invalid',
              'HRIS Extract Status': 'N/A',
              'Department ID': employee.getDepartment(),
              'Department Code': department.codeFor(employee.getDepartment()),
              'Department Name': department.nameFor(employee.getDepartment()),
              'Status': status.nameFor(employee.obj.employee.status),
              'Employee ID': employee.getId(),
              'Employee Last Name': employee.getLastName(),
              'Employee First Name': employee.getFirstName(),
              'Employee Creation Date': employee.obj.employee.creationDate
            });
            return callback(new Error(`Invalid SSN for Taleo employee ${employee.getId()}`));
          }

          // Do a lookup against the ADP extract in SpringCM for this employee
          // by SSN.
          springCm.csvLookup(adpExtract, {
            'SSN': employeeSsn
          }, (err, rows) => {
            if (err) {
              return callback(err);
            }

            // We expect a single row to be returned
            if (rows.length === 0) {
              exceptionData.push({
                'SSN Status': 'Valid',
                'HRIS Extract Status': 'Missing',
                'Department ID': employee.getDepartment(),
                'Department Code': department.codeFor(employee.getDepartment()),
                'Department Name': department.nameFor(employee.getDepartment()),
                'Status': status.nameFor(employee.obj.employee.status),
                'Employee ID': employee.getId(),
                'Employee Last Name': employee.getLastName(),
                'Employee First Name': employee.getFirstName(),
                'Employee Creation Date': employee.obj.employee.creationDate
              });
              return callback(new Error(`Taleo employee ${employee.getId()} missing from ADP extract`));
            } else if (rows.length !== 1) {
              exceptionData.push({
                'SSN Status': 'Valid',
                'HRIS Extract Status': 'Multiple',
                'Department ID': employee.getDepartment(),
                'Department Code': department.codeFor(employee.getDepartment()),
                'Department Name': department.nameFor(employee.getDepartment()),
                'Status': status.nameFor(employee.obj.employee.status),
                'Employee ID': employee.getId(),
                'Employee Last Name': employee.getLastName(),
                'Employee First Name': employee.getFirstName(),
                'Employee Creation Date': employee.obj.employee.creationDate
              });
              return callback(new Error(`Multiple rows in ADP extract for Taleo employee ${employee.getId()}`));
            }

            callback(null, rows[0]);
          });
        },
        (adpEmployee, callback) => {
          /**
           * Get all packets for the employee, then retrieve all activities
           * for each packet. Upload activities to SpringCM.
           */

          taleo.getPackets(employee, (err, packets) => {
            if (err) {
              return callback(err);
            }

            winston.info(`Found ${packets.length} packets for employee ${employee.getId()}`, {
              packets: packets.map(p => p.getId()),
              employeeId: employee.getId(),
              employeeName: employeeName
            });

            // Get all activities for the packet
            async.eachSeries(packets, (packet, callback) => {
              taleo.getActivities(packet, (err, activities) => {
                winston.info(`Found ${activities.length} activities for packet ${packet.getId()}`, {
                  activities: activities.map(a => a.getId()),
                  packet: packet.getId(),
                  employeeId: employee.getId(),
                  employeeName: employeeName
                });

                async.eachSeries(activities, (activity, callback) => {
                  /**
                   * For each activity, determine if the file has already
                   * been uploaded. If an error occurs or the query times
                   * out, skip the file. If not yet delivered, upload the
                   * file to the to-file folder and index it so it routes.
                   */

                  // Pulled from config by lookup against PSID in Taleo
                  var locationName;
                  // SpringCM delivery folder
                  var uploadFolder;

                  // So we can exit the waterfall early if we have to
                  var nextActivity = callback;

                  async.waterfall([
                    (callback) => {
                      models.ActivityExport.findOne({
                        where: {
                          'activity': activity.getId()
                        }
                      }).then(actv => {
                        if (actv) {
                          winston.info(`Activity already delivered: ${activity.getId()}`, {
                            activity: activity.getId(),
                            packet: packet.getId(),
                            employeeId: employee.getId(),
                            employeeName: employeeName
                          });

                          return nextActivity();
                        }

                        callback();
                      }).catch((err) => {
                        console.log(err);
                        return nextActivity();
                      });
                    },
                    (callback) => {
                      /**
                       * Determine the correct delivery folder
                       */

                      var folderPath = '/Admin/Delivery Automation/JIT Deliveries/Human Resources/';

                      springCm.getFolder(folderPath, (err, folder) => {
                        if (err) {
                          return callback(err);
                        }

                        uploadFolder = folder;

                        callback();
                      });
                    },
                    (callback) => {
                      /**
                       * Download the activity to a temp file
                       */
                      tmp.file((err, path, fd, cleanup) => {
                        if (err) {
                          return callback(err);
                        }

                        callback(null, path);
                      });
                    },
                    (tmpPath, callback) => {
                      taleo.downloadActivity(activity, fs.createWriteStream(tmpPath), (err) => {
                        if (err) {
                          return callback(err);
                        }

                        winston.info(`Downloaded activity ${activity.getId()} to ${tmpPath}`, {
                          activity: activity.getId(),
                          packet: packet.getId(),
                          employeeId: employee.getId(),
                          employeeName: employeeName,
                          tmpPath: tmpPath
                        });

                        callback(null, tmpPath);
                      });
                    },
                    (tmpPath, callback) => {
                      springCm.uploadDocument(uploadFolder, fs.createReadStream(tmpPath), (err, doc) => {
                        if (err) {
                          return callback(err);
                        }

                        winston.info(`Uploaded activity ${activity.getId()} to SpringCM`, {
                          activity: activity.getId(),
                          packet: packet.getId(),
                          employeeId: employee.getId(),
                          employeeName: employeeName,
                          remotePath: uploadFolder.getPath()
                        });

                        callback(null, doc);
                      });
                    },
                    (doc, callback) => {
                      var documentName = documentNameFor(activity.getTitle());
                      /**
                       * Index the uploaded document
                       */
                      springCm.setDocumentAttributes(doc, {
                        'Employee Files': {
                          'Employee Data': {
                            'Last Name': {
                              'Value': _.get(adpEmployee, 'Last Name')
                            },
                            'First Name': {
                              'Value': _.get(adpEmployee, 'First Name')
                            },
                            'SSN': {
                              'Value': _.get(adpEmployee, 'SSN')
                            },
                            'SSN (Last 4)': {
                              'Value': _.get(adpEmployee, 'SSN (Last 4)')
                            },
                            'Company Code': {
                              'Value': _.get(adpEmployee, 'Company Code')
                            },
                            'Department Code': {
                              'value': _.get(adpEmployee, 'Department Code')
                            },
                            'Department Name': {
                              'value': _.get(adpEmployee, 'Department Name')
                            },
                            'Status': {
                              'value': _.get(adpEmployee, 'Status')
                            },
                            'Location': {
                              'value': _.get(adpEmployee, 'Location')
                            },
                            'Hire Date': {
                              'value': _.get(adpEmployee, 'Hire Date')
                            },
                            'Termination Date': {
                              'value': _.get(adpEmployee, 'Termination Date')
                            }
                          },
                          'Category': {
                            'Document Name': {
                              'Value': documentName,
                            },
                            'SubCategory': {
                              'Value': subcategoryFor(documentName)
                            },
                            'Category': {
                              'Value': categoryFor(documentName)
                            }
                          }
                        }
                      }, (err) => {
                        if (err) {
                          return callback(err);
                        }

                        winston.info(`Tagged activity ${activity.getId()}`, {
                          activity: activity.getId(),
                          packet: packet.getId(),
                          employeeId: employee.getId(),
                          employeeName: employeeName,
                          attributeGroup: 'Employee Files',
                          adpEmployeeId: _.get(adpEmployee, 'EMP ID'),
                          documentName: activity.getTitle()
                        });

                        callback(null, doc);
                      });
                    },
                    (doc, callback) => {
                      models.ActivityExport.create({
                        'activity': activity.getId(),
                        'page_count': doc.getPageCount(),
                        'activity_title': activity.getTitle(),
                        'employee_id': employee.getId(),
                        'employee_name': employeeName,
                        'exception_upload': 0
                      }).then((row) => {
                        winston.info(`Logged into database: activity ${activity.getId()}`, {
                          activity: activity.getId(),
                          packet: packet.getId(),
                          employeeId: employee.getId(),
                          employeeName: employeeName
                        });

                        callback();
                      }).catch(callback);
                    }
                  ], (err) => {
                    if (err) {
                      winston.error(err.message, err);
                    }

                    callback();
                  });
                }, callback);
              });
            }, (err) => {
              if (err) {
                return callback(err);
              }

              callback(null, adpEmployee);
            });
          });
        },
        (adpEmployee, callback) => {
          /**
           * Get all attachments for the employee, then upload to SpringCM.
           */

          taleo.getAttachments(employee, (err, attachments) => {
            if (err) {
              return callback(err);
            }

            winston.info(`Found ${attachments.length} attachments for employee ${employee.getId()}`, {
              attachments: attachments.map(a => a.getId()),
              employeeId: employee.getId(),
              employeeName: employeeName
            });

            async.eachSeries(attachments, (attachment, callback) => {
              winston.info(`Attachment "${attachment.getDescription()}"`, {
                title: attachment.getDescription(),
                type: attachment.getAttachmentType(),
                employeeId: employee.getId(),
                employeeName: employeeName
              });

              var nextAttachment = callback;

              var uploadFolder;
              var locationName;

              async.waterfall([
                (callback) => {
                  models.AttachmentExport.findOne({
                    where: {
                      'attachment': attachment.getId()
                    }
                  }).then(atch => {
                    if (atch) {
                      winston.info(`Attachment already delivered: ${attachment.getId()}`, {
                        attachment: attachment.getId(),
                        employeeId: employee.getId(),
                        employeeName: employeeName
                      });

                      return nextAttachment();
                    }

                    callback();
                  }).catch(callback);
                },
                (callback) => {
                  /**
                   * Determine the correct delivery folder
                   */

                  var folderPath = '/Admin/Delivery Automation/JIT Deliveries/Human Resources';

                  springCm.getFolder(folderPath, (err, folder) => {
                    if (err) {
                      return callback(err);
                    }

                    uploadFolder = folder;

                    callback();
                  });
                },
                (callback) => {
                  tmp.file((err, path, fd, cleanup) => {
                    if (err) {
                      return callback(err);
                    }

                    callback(null, path);
                  });
                },
                (tmpPath, callback) => {
                  taleo.downloadAttachment(attachment, fs.createWriteStream(tmpPath), (err) => {
                    if (err) {
                      return callback(err);
                    }

                    callback(null, tmpPath);
                  });
                },
                (tmpPath, callback) => {
                  springCm.uploadDocument(uploadFolder, fs.createReadStream(tmpPath), {
                    name: attachment.getFileName(),
                  }, (err, doc) => {
                    if (err) {
                      return callback(err);
                    }

                    winston.info(`Uploaded attachment ${attachment.getId()} to SpringCM`, {
                      attachment: attachment.getId(),
                      employeeId: employee.getId(),
                      employeeName: employeeName,
                      remotePath: uploadFolder.getPath()
                    });

                    callback(null, doc);
                  });
                },
                (doc, callback) => {
                  var documentName;

                  switch (attachment.getAttachmentType()) {
                  case 'Resume_Type':
                    documentName = 'Resume';
                    break;
                  case 'Offer_Type':
                    documentName = 'Offer Letter';
                    break;
                  default:
                    var desc = attachment.getDescription().toLowerCase();

                    if (desc.indexOf('i-9') > -1 || desc.indexOf('i9') > -1) {
                      documentName = 'I9';
                    } else if (desc.indexOf('permanent residence card') > -1 || desc.indexOf('perm res') > -1) {
                      documentName = 'I9';
                    } else if (desc.indexOf('social security card') > -1) {
                      documentName = 'I9';
                    } else if (desc.indexOf('driver\'s license') > -1 || desc.indexOf('driver license') > -1) {
                      documentName = 'I9';
                    } else if (desc.indexOf('birth certificate') > -1 || desc.indexOf('birth cert') > -1) {
                      documentName = 'I9';
                    } else if (desc.indexOf('passport') > -1) {
                      documentName = 'I9';
                    } else if (desc.indexOf('offer letter') > -1) {
                      documentName = 'Offer Letter';
                    } else if (desc.indexOf('Offer letter') > -1) {
                      documentName = 'Offer Letter';
                    } else if (desc.indexOf('voided check') > -1) {
                      documentName = 'Direct Deposit Forms';
                    } else if (desc.indexOf('direct deposit') > -1) {
                      documentName = 'Direct Deposit Forms';
                    } else if (desc.indexOf('rn') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('cna') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('license') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('psv') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('sv') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('bls') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('acls') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('sama') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('mab') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('fire') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('ekg') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('cpt') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('safety') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('aha') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('pals') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('cpi') > -1) {
                      documentName = 'Certs and Licenses';
                    } else if (desc.indexOf('e-verify') > -1) {
                      documentName = 'Confidential Archive';
                    } else if (desc.indexOf('everify') > -1) {
                      documentName = 'Confidential Archive';
                    } else if (desc.indexOf('resume') > -1) {
                      documentName = 'Resume';
                    } else if (desc.indexOf('healthcare') > -1) {
                      documentName = 'Confidential Archive';
                    } else if (desc.indexOf('assessment') > -1) {
                      documentName = 'Confidential Archive';
                    } else if (desc.indexOf('source') > -1) {
                      documentName = 'Confidential Archive';
                    } else if (desc.indexOf('report') > -1) {
                      documentName = 'Confidential Archive';
                    } else if (desc.indexOf('selection') > -1) {
                      documentName = 'Confidential Archive';
                    } else if (desc.indexOf('scf') > -1) {
                      documentName = 'SCF';
                    } else if (desc.indexOf('recommend') > -1) {
                      documentName = 'Resume';
                    } else if (desc.indexOf('recommendation') > -1) {
                      documentName = 'Resume';
                    } else if (desc.indexOf('telephone call recording') > -1) {
                      documentName = 'Call Monitoring';
                    }
                    break;
                  }

                  if (!documentName) {
                    winston.info('Defaulting document name to "Other Docs"', {
                      attachment: attachment.getId(),
                      employeeId: employee.getId(),
                      employeeName: employeeName
                    });

                    documentName = 'Other Docs';
                  } else {
                    winston.info(`Selected document name "${documentName}"`, {
                      attachment: attachment.getId(),
                      employeeId: employee.getId(),
                      employeeName: employeeName,
                      documentName: documentName
                    });
                  }

                  springCm.setDocumentAttributes(doc, {
                    'Employee Files': {
                      'Employee Data': {
                        'Last Name': {
                          'Value': _.get(adpEmployee, 'Last Name')
                        },
                        'First Name': {
                          'Value': _.get(adpEmployee, 'First Name')
                        },
                        'SSN': {
                          'Value': _.get(adpEmployee, 'SSN')
                        },
                        'SSN (Last 4)': {
                          'Value': _.get(adpEmployee, 'SSN (Last 4)')
                        },
                        'Company Code': {
                          'Value': _.get(adpEmployee, 'Company Code')
                        },
                        'Department Code': {
                          'value': _.get(adpEmployee, 'Department Code')
                        },
                        'Department Name': {
                          'value': _.get(adpEmployee, 'Department Name')
                        },
                        'Status': {
                          'value': _.get(adpEmployee, 'Status')
                        },
                        'Location': {
                          'value': _.get(adpEmployee, 'Location')
                        },
                        'Hire Date': {
                          'value': _.get(adpEmployee, 'Hire Date')
                        },
                        'Termination Date': {
                          'value': _.get(adpEmployee, 'Termination Date')
                        }
                      },
                      'Category': {
                        'Document Name': {
                          'Value': documentName
                        },
                        'SubCategory': {
                          'Value': subcategoryFor(documentName)
                        },
                        'Category': {
                          'Value': categoryFor(documentName)
                        }
                      }
                    }
                  }, (err) => {
                    if (err) {
                      return callback(err);
                    }

                    winston.info(`Tagged attachment ${attachment.getId()}`, {
                      attachment: attachment.getId(),
                      employeeId: employee.getId(),
                      employeeName: employeeName,
                      attributeGroup: 'Employee Files',
                      adpEmployeeId: _.get(adpEmployee, 'EMP ID'),
                      documentName: attachment.getDescription()
                    });

                    callback(null, doc);
                  });
                },
                (doc, callback) => {
                  models.AttachmentExport.create({
                    'attachment': attachment.getId(),
                    'page_count': doc.getPageCount(),
                    'employee_id': employee.getId(),
                    'attachment_description': attachment.getDescription(),
                    'attachment_type': attachment.getAttachmentType(),
                    'employee_name': employeeName,
                    'exception_upload': 0
                  }).then((row) => {
                    winston.info(`Logged into database: attachment ${attachment.getId()}`, {
                      attachment: attachment.getId(),
                      employeeId: employee.getId(),
                      employeeName: employeeName
                    });

                    callback();
                  }).catch(callback);
                }
                // Check if already synced
                // Download attachment
                // Upload to routing folder
                // Determine proper document title
                // Index
                // Record into database
              ], callback);
            }, callback);
          });
        }
      ], (err) => {
        if (err) {
          winston.error(err.message, err);
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

    var start = 1;
    var length = 0;
    const _max = 0; // For testing; 0 = no max
    const limit = 25;

    async.doUntil((callback) => {
      taleo.getEmployees({
        start: start,
        limit: limit
      }, (err, employees) => {
        if (err) {
          winston.error(err.message, err);
          return callback();
        }

        length = employees.length;

        if (length === 0) {
          return callback();
        }

        if (length > 1) {
          winston.info(`Queueing employees ${start} through ${start + length}`, {
            start: start,
            end: start + length,
            count: length
          });
        } else if (length === 1) {
          winston.info(`Queueing employee ${start}`, {
            start: start,
            count: 1
          });
        }

        var matches = 0;

        _.each(_.filter(employees, e => validLocations.indexOf(e.getLocation()) > -1), e => {
          matches += 1;
          queue.push(e);
        });

        winston.info(`${matches} matching employees queued`);

        start += length;

        callback();
      });
    }, () => {
      return length === 0 || (_max > 0 && start > _max);
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
  },
  (callback) => {
    fs.writeFile('./PMS-PMH Taleo Exceptions.csv', csvjson.toCSV(exceptionData, {
      delimiter: ',',
      headers: 'key'
    }), callback);
  },
  (callback) => {
    var docPath = '/HR/_Admin - HR/PMS-PMH Taleo Exceptions.csv';
    springCm.checkInDocument(docPath, fs.createReadStream('./PMS-PMH Taleo Exceptions.csv'), {
      filename: 'PMS-PMH Taleo Exceptions.csv'
    }, callback);
  }
], (err) => {
  if (err) {
    winston.error(err.message, err);
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
      winston.error(err.message, err);
      code = 1;
    }

    // Wait for our winston transports to finish streaming data, then exit
    async.parallel([
      (callback) => fileTransport.on('finished', callback),
      (callback) => consoleTransport.on('finished', callback),
      (callback) => {
        if (cwlTransport) {
          cwlTransport.kthxbye(callback)
        } else {
          callback();
        }
      }
    ], () => {
      process.exit(code);
    });
  });
});
