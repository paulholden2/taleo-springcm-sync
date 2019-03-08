module.exports = {
  nameFor: function (id) {
    switch (parseInt(id)) {
      case 1: return 'Employed';
      case 2: return 'Terminated';
      case 6: return 'Pre-Boarding';
      default: return 'Unknown';
    }
  }
};
