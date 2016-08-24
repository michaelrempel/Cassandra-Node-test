"use strict";
var cassandra = require('cassandra-driver');
var assert = require('assert');
var BigDecimal = require('cassandra-driver').types.BigDecimal;
var Uuid = require('cassandra-driver').types.Uuid;
var TimeUuid = require('cassandra-driver').types.TimeUuid;


//out of the box Cassandra has no need for authProvider.
//commment out the two authProvider lines here if your install doesnt use authentication.
var authProvider = new cassandra.auth.PlainTextAuthProvider('cassandra','cassandra');
var client = new cassandra.Client({
    contactPoints: ['127.0.0.1']
    ,authProvider:authProvider
});
var vars=[]; //push all your read it later vars into this array.
vars.push({key:'test',value:'Something'});
// Run the steps. This exits when done.
step1(vars);

function step1(vars){  
    client.connect(function (err) {
    if (err) { 
        client.shutdown();
        return console.error(err);
    }
    console.log('Connected to cluster with %d host(s): %j', client.hosts.length, client.hosts.keys());
    step2(vars);
    });
}
//client.execute('SELECT key FROM system.local', function (err, result) {
function step2(vars){ 
//Some types
    var myTimeId = cassandra.types.TimeUuid.now() //new instance based on this machine's current date
    var myDate = myTimeId.getDate(); //date representation.
    vars.push({key:'myTimeID',value:myTimeId});
    vars.push({key:'myDate',value:myDate});

    console.log('Keyspaces: %j', Object.keys(client.metadata.keyspaces));
    console.log('Some playing with integers. Bit manipulation for meta table replacement.');
    let x=1;
    console.log('x='+x);
    x=x<<3;
    console.log('x=x<<3');
    console.log('x='+x);
    console.log('x|1='+(x|1));
    console.log('((x&8)===8) is '+((x&8)===8));    
    step3(vars);
}
// Named parameters seqNamedParams1
function step3(vars){ 
    console.log('Named parameters section. Insert test, single row');
    var myUuid = cassandra.types.Uuid.random(); //new uuid v4 mostly wont collide.
    vars.push({key:'myUuid',value:myUuid});
    var myQuery = 'INSERT INTO "datatypes"."testInteger" (id,column1) VALUES(?,?)';
    var myParams = { id: myUuid, column1:1};
    client.execute(myQuery,myParams,{prepare: true}, function(err) {
        assert.ifError(err);
        console.log('complete.');
        step5(vars); //skip 4, not working.
    });
};
//Batch processing. THIS IS NOT WORKING. v3 driver is different. TBD.
function step4(vars){ 
    console.log('Batch processing section. Insert test, multi row');
    var myUuid2 = cassandra.types.Uuid.random();
    var myUuid3 = cassandra.types.Uuid.random();
    var myQuery2 = 'INSERT INTO datatypes."testInteger" (id,column1) VALUES(?,?)';
    var myQuery3 = 'INSERT INTO datatypes."testInteger" (id) VALUES(?)';
    var myQueries = [
        {query:myQuery2, params:[myUuid2,2]},
        {query:myQuery3, params:[myUuid3]}
    ];

    client.batch(myQueries, {preare:true}, function(err){
        //all queries have been executed successfully
        // or none of the changes have been applied
        assert.ifError(err);
        console.log('complete.');
        step5(vars);
    });
};
function step5(vars){
    console.log('Partial processing section. Insert test, no integer');
    var myUuid = cassandra.types.Uuid.random();
    vars.push({key:'myUuid5',value:myUuid});
    var myQuery = 'INSERT INTO datatypes."testInteger" (id) VALUES(?)';     
    var myParams = { id: myUuid};
    client.execute(myQuery,myParams,{prepare: true}, function(err) {
        assert.ifError(err);
        console.log('complete.');
        step6(vars); 
    });     
};
function step6(vars){
  var myUuid = getValue(vars,'myUuid');
  console.log('Select myUuid from named parameters insert above');
  console.log(myUuid);
  client.execute('SELECT * FROM datatypes."testInteger" WHERE id in(?)',{id:myUuid},{prepare:true, hints:['Uuid']}, function(err,result){
      assert.ifError(err);
      var row = result.first();
      console.log('returned values:')
      console.log(result.rows[0]['id']);
      console.log(result.rows[0]['column1']);
//      console.log(row['id']); //same thing
      console.log('complete.');
      step7(vars);
  });
};
function step7(vars) {
//finaly test a bunch of stuff at once.
//BLOB not tested here.
    var cqlInsertSampleData =
        "INSERT INTO datatypes.sample (" +
        "   primaryRowKey, secondaryRowKey, " +
        "   primaryColumnKey, secondaryColumnKey," +
        "   atime, atimeuuid, aboolean, abigint," +
        "   auuid, adouble, adecimal," +
        "   aset, amap, alist" +
        ") VALUES (?,?, ?,?, ?,?,?,?, ?,?,?, ?,?,?)";
 
    var aset = ["abc", "def", "ghi"];
    var alist = ["xxx", "jjj", "ttt"];
    var amap = {"key1": "value1", "key2": "value2"};
 // Note: The optional ‘hints‘ below are only necessary to tell the driver where the object is type ‘map‘. 
 // Without this hint (or indeed the entire ‘options’ argument) the driver blows with an error like this: 
 //   Target data type could not be guessed, you must specify a hint for value…. 
 // It took a while to find this out because the JSON.stringify() didn’t output the ‘result.message’ properly 
 // (for some reason I’m not yet aware of), and because the sample code for NodeJS driver usage doesn’t go into 
 // handling of maps.  Some legacy code suggests you can inline this hint but that doesn’t work either.
 // In fact a lot of the documentation leaves a great deal to be desired. It seems to be a tradition with
 // Node, Typescript, etc. which also carries into the books.

    const aBigDecimal = new BigDecimal.fromString('1.1234567890123456789');
    const aTimeUuid = new TimeUuid.now();
    client.execute(
        cqlInsertSampleData,
        [   'row2', 'part1', cassandra.types.timeuuid(), 'columnGroup',
            new Date(),aTimeUuid, true, new cassandra.types.Long.fromString("876543210987654321"),
            cassandra.types.uuid(), 1.5, 1.1234567890123456789,
            aset, amap, alist
        ],
        {hints: [
            null, null, null, null,
            null, null, null, null,
            null, null, null,
            null, 'map', null
        ]},
        function (err, result) {
            if (err) {
                console.log('Insert Sample Data Failed: ' + JSON.stringify(err));
                // needed to add this to get the error message as sometimes stringify didn't include it
                console.log('Insert Sample Data Failed: ' + err.message);
            } else {
                console.log('Inserted Sample Data: ' + JSON.stringify(result));
                vars.push({key:'stuff',value:result});
                console.log('complete.');                
                step8(vars)
            }
        }
    );

};
function step8(vars){

    // Comparison to inserted values is possible. I didnt do it.
    // Decimal is giving trouble, it is interpreted as a string that is too long.
var cqlSelectSampleData =
        "SELECT * FROM datatypes.sample " +
        "WHERE primaryRowKey = ? and secondaryRowKey = ?";
 
    client.execute(
        cqlSelectSampleData, ['row2','part1'],
        function (err, result) {
            if (err) {
                console.log('Select Sample Data Failed: ' + JSON.stringify(err));
                console.log('Select Sample Data Failed: ' + err.message);
            } else {
//                console.log('Selected Sample Data: ' + JSON.stringify(result.rows[0])); //too big for parser
                // notice the names are all lowercase
                for( var i=0; i<result.rows.length; i++ ){
                    console.log('Row '+(i+1)+' of '+result.rows.length);
                    console.log('primaryRowKey      : ' + result.rows[i]['primaryrowkey']);
                    console.log('secondaryRowKey    : ' + result.rows[i]['secondaryrowkey']);
                    console.log('primaryColumnKey   : ' + result.rows[i]['primarycolumnkey']);
                    console.log('secondaryColumnKey : ' + result.rows[i]['secondarycolumnkey']);
    
                    console.log('atime              : ' + new Date(result.rows[i]['atime']));
                    console.log('atimeuuid          : ' + result.rows[i]['atimeuuid']);
                    console.log('aboolean           : ' + result.rows[i]['aboolean']);
                    console.log('abigint            : ' + result.rows[i]['abigint']);
    
                    console.log('auuid              : ' + result.rows[i]['auuid']);
    //                var aDecimal = result.rows[0]['adecimal'];
    //                console.log('adecimal           : ' + aDecimal.toString); //was .readDoubleBE(0));
                    console.log('adouble            : ' + result.rows[i]['adouble']);
    
                    console.log('aset               : ' + result.rows[i]['aset']);
                    console.log('amap               : ' + JSON.stringify(result.rows[i]['amap']));
                    console.log('alist              : ' + result.rows[i]['alist']);
                    console.log('');
                }
                console.log('complete.');
                step99();
            }
        }
    );

};


function step99(){
    console.log('All Done, no failures. Exiting.');     
    client.shutdown();
    process.exit(0);
};

function getValue(vars,key) {
    //looks up key, returns Value
    for(var i=0; i<vars.length;i++){
       if (vars[i].key===key){
           return vars[i].value;
       } 
    } 
}
