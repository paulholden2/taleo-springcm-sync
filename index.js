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

    var adpExtractPath = '/PMH/Alta Hospitals/Human Resources/_Admin/Employee Information.csv';

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
          var eeid = employee.obj.EEID;

          // Skip if no SSN
          if (!eeid || eeid === '') {
            return callback(new Error(`Missing EEID for Taleo employee ${employee.getId()}`));
          }
          // Do a lookup against the ADP extract in SpringCM for this employee
          // by SSN.
          springCm.csvLookup(adpExtract, {
            'EMP ID': eeid
          }, (err, rows) => {
            if (err) {
              return callback(err);
            }

            // We expect a single row to be returned
            if (rows.length === 0) {
              return callback(new Error(`Taleo employee ${employee.getId()} missing from ADP extract`));
            } else if (rows.length !== 1) {
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
                if (err) {
                  return callback(err);
                }

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
                      }).catch(callback);
                    },
                    (callback) => {
                      /**
                       * Map the employee's PSID in ADP to their location
                       * name, for which we can retrieve the delivery folder.
                       */

                      var psid = _.get(adpEmployee, 'PSID');
                      var locations = _.get(conf, 'taleo-springcm-sync.taleo.locations');
                      var matches = _.filter(locations, loc => _.get(loc, 'psid').toLowerCase() === psid.toLowerCase());

                      if (matches.length === 0) {
                        return callback(new Error('No locations found for ADP PSID ' + psid));
                      } else if (matches.length > 1) {
                        return callback(new Error('Multiple locations found for ADP PSID ' + psid));
                      }

                      locationName = _.get(matches, '[0].name');

                      callback();
                    },
                    (callback) => {
                      /**
                       * Determine the correct delivery folder
                       */

                      var folderPath = `/PMH/Alta Hospitals/Human Resources/${locationName}/_Documents_To_File`;

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
                      /**
                       * Index the uploaded document
                       */

                      if (locationName === 'Culver City') {
                        springCm.setDocumentAttributes(doc, {
                          'PMH Employee File - Culver City': {
                            'Employee Information': {
                              'Last Name': {
                                'Value': _.get(adpEmployee, 'Last Name')
                              },
                              'First Name': {
                                'Value': _.get(adpEmployee, 'First Name')
                              },
                              'EMP ID': {
                                'Value': _.get(adpEmployee, 'EMP ID')
                              }
                            },
                            'Document Information': {
                              'Document Name': {
                                'Value': activity.getTitle()
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
                            attributeGroup: 'PMH Employee File - Culver City',
                            adpEmployeeId: _.get(adpEmployee, 'EMP ID'),
                            documentName: activity.getTitle()
                          });

                          callback(null, doc);
                        });
                      } else {
                        springCm.setDocumentAttributes(doc, {
                          'PMH Employee File - Alta HR': {
                            'Employee Information': {
                              'Last Name': {
                                'Value': _.get(adpEmployee, 'Last Name')
                              },
                              'First Name': {
                                'Value': _.get(adpEmployee, 'First Name')
                              },
                              'EMP ID': {
                                'Value': _.get(adpEmployee, 'EMP ID')
                              }
                            },
                            'Document Information': {
                              'Document Name': {
                                'Value': activity.getTitle()
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
                            attributeGroup: 'PMH Employee File - Alta HR',
                            adpEmployeeId: _.get(adpEmployee, 'EMP ID'),
                            documentName: activity.getTitle()
                          });

                          callback(null, doc);
                        });
                      }
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
                   * Map the employee's PSID in ADP to their location
                   * name, for which we can retrieve the delivery folder.
                   */

                  var psid = _.get(adpEmployee, 'PSID');
                  var locations = _.get(conf, 'taleo-springcm-sync.taleo.locations');
                  var matches = _.filter(locations, loc => _.get(loc, 'psid') === psid);

                  if (matches.length === 0) {
                    return callback(new Error('No locations found for ADP PSID ' + psid));
                  } else if (matches.length > 1) {
                    return callback(new Error('Multiple locations found for ADP PSID ' + psid));
                  }

                  locationName = _.get(matches, '[0].name');

                  callback();
                },
                (callback) => {
                  /**
                   * Determine the correct delivery folder
                   */

                  var folderPath = `/PMH/Alta Hospitals/Human Resources/${locationName}/_Documents_To_File`;

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
                    } else if (desc.indexOf('voided') > -1 || desc.indexOf('voided check') > -1) {
                      documentName = 'Confidential';
                    } else if (desc.indexOf('everify') > -1 || desc.indexOf('e-verify') > -1) {
                      documentName = 'Confidential';
                    } else if (desc.indexOf('healthcare') > -1 || desc.indexOf('health assessment') > -1 || desc.indexOf('health assess') > -1) {
                      documentName = 'Confidential';
                    } else if (desc.indexOf('license') > -1 || desc.indexOf('licensure') > -1 || desc.indexOf('certificate') > -1) {
                      documentName = 'License Certification and Education';
                    } else if (desc.indexOf('primary source verification') > -1 || desc.indexOf('psv') > -1) {
                      documentName = 'License Certification and Education';
                    } else if (desc.indexOf('masters') > -1 || desc.indexOf('bachelors') > -1 || desc.indexOf('education') > -1 || desc.indexOf('degree') > -1) {
                      documentName = 'License Certification and Education';
                    } else if (desc.indexOf('license') > -1 || desc.indexOf('cna') > -1 || desc.indexOf('bls') > -1 || desc.indexOf('acls') > -1 || desc.indexOf('cpt') > -1 || desc.indexOf('safety') > -1 || desc.indexOf('cpi') > -1) {
                      documentName = 'License Certification and Education';
                    } else if (desc.indexOf('job description') > -1) {
                      documentName = 'Job Descriptions and Evaluations';
                    } else if (desc.indexOf('job desc') > -1) {
                      documentName = 'Job Descriptions and Evaluations';
                    }
                    break;
                  }

                  if (!documentName) {
                    winston.info('Defaulting document name to "Personal Information - Other"', {
                      attachment: attachment.getId(),
                      employeeId: employee.getId(),
                      employeeName: employeeName
                    });

                    documentName = 'Personal Information - Other';
                  } else {
                    winston.info(`Selected document name "${documentName}"`, {
                      attachment: attachment.getId(),
                      employeeId: employee.getId(),
                      employeeName: employeeName,
                      documentName: documentName
                    });
                  }

                  if (locationName === 'Culver City') {
                    springCm.setDocumentAttributes(doc, {
                      'PMH Employee File - Culver City': {
                        'Employee Information': {
                          'Last Name': {
                            'Value': _.get(adpEmployee, 'Last Name')
                          },
                          'First Name': {
                            'Value': _.get(adpEmployee, 'First Name')
                          },
                          'EMP ID': {
                            'Value': _.get(adpEmployee, 'EMP ID')
                          }
                        },
                        'Document Information': {
                          'Document Name': {
                            'Value': documentName
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
                        attributeGroup: 'PMH Employee File - Culver City',
                        adpEmployeeId: _.get(adpEmployee, 'EMP ID'),
                        documentName: attachment.getDescription()
                      });

                      callback(null, doc);
                    });
                  } else {
                    springCm.setDocumentAttributes(doc, {
                      'PMH Employee File - Alta HR': {
                        'Employee Information': {
                          'Last Name': {
                            'Value': _.get(adpEmployee, 'Last Name')
                          },
                          'First Name': {
                            'Value': _.get(adpEmployee, 'First Name')
                          },
                          'EMP ID': {
                            'Value': _.get(adpEmployee, 'EMP ID')
                          }
                        },
                        'Document Information': {
                          'Document Name': {
                            'Value': documentName
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
                        attributeGroup: 'PMH Employee File - Alta HR',
                        adpEmployeeId: _.get(adpEmployee, 'EMP ID'),
                        documentName: attachment.getDescription()
                      });

                      callback(null, doc);
                    });
                  }
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

        _.each(_.filter(employees, e => {
          if (validLocations.indexOf(e.getLocation()) < 0) {
            return false;
          }

          if (e.obj.EEID === "") {
            winston.info(`Skipping employee ${e.getId()} (missing EEID)`);
            return false;
          }

          if (e.obj.startDate == null) {
            winston.info(`Skipping employee ${e.getId()} (no start date)`);
            return false;
          }

          if (e.obj.onBoardStatus !== 4) {
            winston.info(`Skipping employee ${e.getId()} (onboarding not complete)`);
            return false;
          }

          return true;
        }), e => queue.push(e));

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
    // Skip
    return callback();
    fs.writeFile('./Alta Taleo Exceptions.csv', csvjson.toCSV(exceptionData, {
      delimiter: ',',
      headers: 'key'
    }), callback);
  },
  (callback) => {
    // Skip
    return callback();
    var docPath = '/PMH/Alta Hospitals/Human Resources/_Admin/Taleo Sync/Alta Taleo Exceptions.csv';
    springCm.checkInDocument(docPath, fs.createReadStream('./Alta Taleo Exceptions.csv'), {
      filename: 'Alta Taleo Exceptions.csv'
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
