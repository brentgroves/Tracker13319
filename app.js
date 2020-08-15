const mariadb = require("mariadb");
const mqtt = require('mqtt');
const common = require('@bgroves/common');




const {
  MQTT_SERVER,
  MYSQL_HOSTNAME,
  MYSQL_USERNAME,
  MYSQL_PASSWORD,
  MYSQL_DATABASE
} = process.env;
/*
const MQTT_SERVER='localhost';
const MYSQL_HOSTNAME= "localhost";
const MYSQL_USERNAME= "brent";
const MYSQL_PASSWORD= "JesusLives1!";
const MYSQL_DATABASE= "mach2";
*/
const connectionString = {
  connectionLimit: 5,
  multipleStatements: true,
  host: MYSQL_HOSTNAME,
  user: MYSQL_USERNAME,
  password: MYSQL_PASSWORD,
  database: MYSQL_DATABASE
}

common.log(`user: ${MYSQL_USERNAME},password: ${MYSQL_PASSWORD}, database: ${MYSQL_DATABASE}, MYSQL_HOSTNAME: ${MYSQL_HOSTNAME}`);

const pool = mariadb.createPool( connectionString);



async function ToolChange(CNC_Key,Part_key,Assembly_Key,Actual_Tool_Life,Trans_Date) {
  let conn;
  try {
    conn = await pool.getConnection();      
    const someRows = await conn.query('call InsKep13319(?,?,?,?,?,?,?,?,?)',[nodeId,name,plexus_Customer_No,pcn, workcenter_Key,workcenter_Code,cnc,value,transDate]);
    let msgString = JSON.stringify(someRows[0]);
    const obj = JSON.parse(msgString.toString()); // payload is a buffer
    common.log(obj);
  } catch (err) {
    // handle the error
    console.log(`Error =>${err}`);
  } finally {
    if (conn) conn.release(); //release to pool
  }
}

async function TrackerUpdateCurrentValue(CNC_Key,Part_key,Assembly_Key,Current_Value,Trans_Date) {
  let conn;
  try {
    conn = await pool.getConnection();      
    const someRows = await conn.query('call TrackerUpdateCurrentValue(?,?,?,?,?,?,?,?,@ReturnValue); select @ReturnValue as pReturnValue',[CNC_Key,Part_key,Assembly_Key,Current_Value,Trans_Date]);
    let returnValue = someRows[1][0].pReturnValue
    common.log(`TrackerUpdateCurrentValue.returnValue=${returnValue}`);
  } catch (err) {
    // handle the error
    console.log(`Error =>${err}`);
  } finally {
    if (conn) conn.release(); //release to pool
  }
}


function main() {
  common.log(`MQTT_SERVER=${MQTT_SERVER}`);
  const mqttClient = mqtt.connect(`mqtt://${MQTT_SERVER}`);

  mqttClient.on('connect', function() {
    mqttClient.subscribe('ToolChange', function(err) {
      if (!err) {
        common.log('Tracker13319 has subscribed to: ToolChange');
      }
    });
  });

  // message is a buffer
  mqttClient.on('message', function(topic, message) {
    const obj = JSON.parse(message.toString()); // payload is a buffer
    common.log(`Tracker13319 => ${message.toString()}`);

    switch(topic) {
      case 'ToolChange':
       // ToolChange(obj.CNC_Key,obj.Part_key,obj.Assembly_Key,obj.Actual_Tool_Life,obj.Trans_Date);      
        break;
      case 'TrackerUpdateCurrentValue':
        CounterUpdate(obj.CNC_Key,obj.Part_key,obj.Assembly_Key,obj.Current_Value,obj.Trans_Date);      
        break;
      default:
        // code block
    }

  });
  
}
main();
