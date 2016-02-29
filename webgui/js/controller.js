var ddContent = angular.module('ddContent', ['ngResource'])
.controller('contentController', ContentController);

ddContent.factory('test', function($resource) {
  return $resource("http://127.0.0.1:5000/test",{ }, {
    getData: {method:'GET', isArray: false}
  });
});

ddContent.factory('kwsearch', function($resource) {
  return $resource("http://127.0.0.1:5000/kwsearch",{ }, {
    getData: {method:'GET', isArray: true}
  });
});

ddContent.factory('colsim', function($resource) {
  return $resource("http://127.0.0.1:5000/colsim",{ }, {
    getData: {method:'GET', isArray: false}
  });
});

ddContent.factory('colove', function($resource) {
  return $resource("http://127.0.0.1:5000/colove",{ }, {
    getData: {method:'GET', isArray: false}
  });
});

function ContentController(test, kwsearch, colsim, colove) {
  /*
    The files returned by the last user action. These are grouped by rows for
    visualization in the left panel.
  */
  this.test = test;
  this.kwsearch = kwsearch;
  this.colsim = colsim;
  this.colove = colove;
  this.rows = [];
  this.rowss = [
    {'files': [
      {'filename': 'A',
      'schema': [
        {'colname': 'id1', 'samples': ['1', '1', '1'], 'selected': 'Y'},
        {'colname': 'name1', 'samples': ['one', 'one'], 'selected': 'N'}
      ]},
      {'filename': 'B',
      'schema': [
        {'colname': 'id2', 'samples': ['2', '2', '2'], 'selected': 'Y'},
        {'colname': 'name2', 'samples': ['two', 'two', 'two'], 'selected': 'N'}
      ]},
      {'filename': 'C',
      'schema': [
        {'colname': 'id3', 'samples': ['3', '3', '3'], 'selected': 'N'},
        {'colname': 'name3', 'samples': ['three', 'three', 'three'], 'selected':'N'}
      ]},
      {'filename': 'D',
      'schema': [
        {'colname': 'id4', 'samples': ['0', '0', '0'], 'selected':'Y'},
        {'colname': 'name4', 'samples': ['0', '0', '0'], 'selected':'N'}
      ]}
    ]},
    {'files': [
      {'filename': 'E',
      'schema': [
        {'colname': 'id5', 'samples': ['0', '0', '0'], 'selected': 'N'},
        {'colname': 'name5', 'samples': ['0', '0', '0'], 'selected': 'N'}
      ]},
      {'filename': 'F',
      'schema': [
        {'colname': 'id6', 'samples': ['0', '0', '0'], 'selected':'N'},
        {'colname': 'name6', 'samples': ['0', '0', '0'], 'selected':'N'}
      ]},
      {'filename': 'G',
      'schema': [
        {'colname': 'id7', 'samples': ['0', '0', '0'], 'selected': 'N'},
        {'colname': 'name7', 'samples': ['0', '0', '0'], 'selected': 'N'}
      ]},
      {'filename': 'H',
      'schema': [
        {'colname': 'id8', 'samples': ['0', '0', '0'], 'selected': 'N'},
        {'colname': 'name8', 'samples': ['0', '0', '0'], 'selected': 'N'}
    ]}
    ]},
    {'files': [
      {'filename': 'I',
      'schema': [
        {'colname': 'id9', 'samples': ['0', '0', '0'], 'selected': 'Y'},
        {'colname': 'name9', 'samples': ['0', '0', '0'], 'selected': 'Y'}
      ]},
      {'filename': 'J',
      'schema': [
        {'colname': 'id10', 'samples': ['0', '0', '0'], 'selected': 'N'},
        {'colname': 'name10', 'samples': ['0', '0', '0'], 'selected': 'N'}
      ]},
      {'filename': 'K',
      'schema': [
        {'colname': 'id11', 'samples': ['0', '0', '0'], 'selected': 'N'},
        {'colname': 'name11', 'samples': ['0', '0', '0'], 'selected': 'N'}
      ]}
    ]}
  ];

  /*
    The content shown in the right panel, associated to a given file
  */
  this.schema = [

  ];

  this.selectedFile;
  this.selectedColumn;
  this.samples = "Simple test, is this overwritten already?";

  this.numberItemsPerRow = 4;
}

/*
Format list of items into list of rows with n items (files) per row
*/
ContentController.prototype.formatServerResultIntoClientFormat = function(serverResult) {
  var newRows = [];
  var currentRow = [];
  for(var i = 0; i < serverResult.length; i++) {
    currentRow.push(serverResult[i]);
    if(currentRow.length == this.numberItemsPerRow) {
      // Finished row
      newRows.push({'files': currentRow});
      currentRow = [];
    }
  }
  if(currentRow.length != 0) {
    newRows.push({'files': currentRow});
  }
  this.rows = newRows;
};

ContentController.prototype.setRowsTest = function () {
  $scope = this; // wrapping the scope for the closure
  var result = this.test.get(function() {
    var newRows = result.result;
    $scope.rows = newRows;
  });
};

ContentController.prototype.keywordSearch = function (keyword) {
  $scope = this; // wrapping the scope for the closure
  var result = this.kwsearch.get({'kw': keyword}, function() {
    var serverResult = result.result;
    $scope.formatServerResultIntoClientFormat(serverResult);
  });
};

/*
  On file click, we change the schema that is shown
*/
ContentController.prototype.setSchema = function (filename) {
  this.selectedColumn = null;
  this.selectedFile = filename;
  for(var i = 0; i<this.rows.length; i++) {
    var files = this.rows[i].files;
    for(var j = 0; j<files.length; j++) {
      var file = files[j];
      if(file.filename == filename) {
        this.schema = file.schema;
      }
    }
  }
};

/*
  On schema.column click, we show some samples of the values
*/
ContentController.prototype.showSamples = function (colname) {
  this.samples = "overwrite at the very beginning";
  for(var i = 0; i<this.schema.length; i++) {
    var col = this.schema[i];
    if(col.colname == colname) {
      var smps = col.samples;
      this.samples = smps;
      $('#modaldialog').modal();
    }
  }

};

ContentController.prototype.selectColumn = function (colname) {
  console.log("selected: " + colname);
  this.selectedColumn = colname;
};

ContentController.prototype.colSim = function () {
  if(this.selectedFile == 'undefined' || this.selectedFile == null
  || this.selectedColumn == 'undefined' || this.selectedColumn == null) {
    alert("SELECT a file and a column FIRST");
    return;
  }
  this.schema = null;
  key = {'filename': this.selectedFile, 'columname': this.selectedColumn};
  var result = this.colsim.get(
    {'filename': this.selectedFile, 'colname': this.selectedColumn}, function() {
      var serverResult = result.result;
      $scope.formatServerResultIntoClientFormat(serverResult);
  });
};

ContentController.prototype.colOve = function () {
  if(this.selectedFile == 'undefined' || this.selectedFile == null
  || this.selectedColumn == 'undefined' || this.selectedColumn == null) {
    alert("SELECT a file and a column FIRST");
    return;
  }
  this.schema = null;
  key = {'filename': this.selectedFile, 'columname': this.selectedColumn};
  var result = this.colove.get(
    {'filename': this.selectedFile, 'colname': this.selectedColumn}, function() {
      var serverResult = result.result;
      $scope.formatServerResultIntoClientFormat(serverResult);
  });
};

ContentController.prototype.selectStyleColumn = function (selected) {
  if(selected == 'Y') {
    return {'background-color':'blue'};
  }
  else {
    return '{}';
  }
};
