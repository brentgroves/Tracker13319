const mariadb = require("mariadb");
const mqtt = require('mqtt');
const common = require('@bgroves/common');




const {
  MQTT_SERVER,
  MQTT_PORT,
  MYSQL_HOSTNAME,
  MYSQL_PORT,
  MYSQL_USERNAME,
  MYSQL_PASSWORD,
  MYSQL_DATABASE
} = process.env;

/*
const MQTT_SERVER='localhost';
const MQTT_PORT='1882';
const MYSQL_HOSTNAME= "localhost";
const MYSQL_PORT='3305';
const MYSQL_USERNAME= "brent";
const MYSQL_PASSWORD= "JesusLives1!";
const MYSQL_DATABASE= "mach2";
*/

const connectionString = {
  connectionLimit: 5,
  multipleStatements: true,
  host: MYSQL_HOSTNAME,
  port: MYSQL_PORT,
  user: MYSQL_USERNAME,
  password: MYSQL_PASSWORD,
  database: MYSQL_DATABASE
}

common.log(`user: ${MYSQL_USERNAME},password: ${MYSQL_PASSWORD}, database: ${MYSQL_DATABASE}, MYSQL_HOSTNAME: ${MYSQL_HOSTNAME}, MYSQL_PORT: ${MYSQL_PORT}`);

const pool = mariadb.createPool( connectionString);

/*
set @CNC_Part_Operation_Key = 1;
set @Set_No = 1;
set @Block_No = 1;
set @Actual_Tool_Life = 2;
set @Trans_Date = '2020-08-18 00:00:01';
-- CNC_Part_Operation_Key=1,Set_No=1,Block_No=1,Current_Value=18136,Last_Update=2020-08-25 10:38:27
-- "CNC_Part_Operation_Key":1,"Set_No":1,"Block_No":7,"Current_Value":29392,"Trans_Date":"2020-08-25 10:17:55"
-- select a.* from CNC_Part_Operation_Assembly a
CALL InsToolAssemblyChangeHistory(@CNC_Part_Operation_Key,@Set_No,@Block_No,@Actual_Tool_Life,@Trans_Date,@Tool_Assembly_Change_History_Key,@Return_Value);
	 -- UpdateCNCPartOperationAssemblyCurrentValue(?,?,?,?,?,@ReturnValue); select @ReturnValue as pReturnValue
SELECT @Tool_Assembly_Change_History_Key,@Return_Value;

*/

async function InsToolAssemblyChangeHistory(CNC_Part_Operation_Key,Set_No,Block_No,Actual_Tool_Assembly_Life,Trans_Date) {
  let conn;
  try {
    conn = await pool.getConnection();      
    console.log(`In InsToolAssemblyChangeHistory with params CNC_Part_Operation_Key=${CNC_Part_Operation_Key},Set_No=${Set_No},Block_No=${Block_No},Actual_Tool_Assembly_Life=${Actual_Tool_Assembly_Life},Trans_Date=${Trans_Date}`)
    // const someRows = await conn.query('call UpdateCNCPartOperationAssemblyCurrentValue(1,1,1,6,"2020-08-25 10:17:55",@ReturnValue); select @Tool_Assembly_Change_History_Key as pTool_Assembly_Change_History_Key, @ReturnValue as pReturnValue');
    const someRows = await conn.query('call InsToolAssemblyChangeHistory(?,?,?,?,?,@Tool_Assembly_Change_History_Key,@Return_Value); select @Tool_Assembly_Change_History_Key as pTool_Assembly_Change_History_Key,@Return_Value as pReturn_Value',[CNC_Part_Operation_Key,Set_No,Block_No,Actual_Tool_Assembly_Life,Trans_Date]);
    let returnValue = someRows[1][0].pReturn_Value;
    let toolAssemblyChangeHistoryKey = someRows[1][0].pTool_Assembly_Change_History_Key;
    console.log(`InsToolAssemblyChangeHistory.returnValue=${returnValue}`);
    console.log(`InsToolAssemblyChangeHistory.toolAssemblyChangeHistoryKey=${toolAssemblyChangeHistoryKey}`);
  } catch (err) {
    // handle the error
    console.log(`Error =>${err}`);
  } finally {
    if (conn) conn.release(); //release to pool
  }
}
/*
async function UpdateCNCPartOperationAssemblyCurrentValue(CNC_Part_Operation_Key,Set_No,Block_No,Current_Value,Last_Update) {
  let conn;
  try {
    conn = await pool.getConnection();      
    common.log(`In UpdateCNCPartOperationAssemblyCurrentValue with params CNC_Part_Operation_Key=${CNC_Part_Operation_Key},Set_No=${Set_No},Block_No=${Block_No},Current_Value=${Current_Value},Last_Update=${Last_Update}`)
    const someRows = await conn.query('call UpdateCNCPartOperationAssemblyCurrentValue(?,?,?,?,?,@ReturnValue); select @ReturnValue as pReturnValue',[CNC_Part_Operation_Key,Set_No,Block_No,Current_Value,Last_Update]);
    let returnValue = someRows[1][0].pReturnValue
    common.log(`UpdateCNCPartOperationAssemblyCurrentValue.returnValue=${returnValue}`);
  } catch (err) {
    // handle the error
    console.log(`Error =>${err}`);
  } finally {
    if (conn) conn.release(); //release to pool
  }
}
*/
async function UpdateCNCPartOperationAssembly(CNC_Part_Operation_Key,Set_No,Block_No,Current_Value,Last_Update) {
  let conn;
  try {
    conn = await pool.getConnection();      
    console.log(`In UpdateCNCPartOperationAssembly with params CNC_Part_Operation_Key=${CNC_Part_Operation_Key},Set_No=${Set_No},Block_No=${Block_No},Current_Value=${Current_Value},Last_Update=${Last_Update}`)
    // const someRows = await conn.query('call UpdateCNCPartOperationAssemblyCurrentValue(1,1,1,6,"2020-08-25 10:17:55",@ReturnValue); select @ReturnValue as pReturnValue');
    const someRows = await conn.query('call UpdateCNCPartOperationAssembly(?,?,?,?,?,@ReturnValue); select @ReturnValue as pReturnValue',[CNC_Part_Operation_Key,Set_No,Block_No,Current_Value,Last_Update]);
    let returnValue = someRows[1][0].pReturnValue
    console.log(`UpdateCNCPartOperationAssembly.returnValue=${returnValue}`);
  } catch (err) {
    // handle the error
    console.log(`Error =>${err}`);
  } finally {
    if (conn) conn.release(); //release to pool
  }
}


function main() {
  common.log(`MQTT_SERVER=${MQTT_SERVER},MQTT_PORT=${MQTT_PORT}`);
  const mqttClient = mqtt.connect(`mqtt://${MQTT_SERVER}:${MQTT_PORT}`);

  mqttClient.on('connect', function() {
    mqttClient.subscribe('InsToolAssemblyChangeHistory', function(err) {
      if (!err) {
        common.log('Tracker13319 has subscribed to: InsToolAssemblyChangeHistory');
      }
    });
    mqttClient.subscribe('UpdateCNCPartOperationAssembly', function(err) {
      if (!err) {
        common.log('Tracker13319 has subscribed to: UpdateCNCPartOperationAssembly');
      }
    });
  });

  // message is a buffer
  mqttClient.on('message', function(topic, message) {
    const obj = JSON.parse(message.toString()); // payload is a buffer
    common.log(`Tracker13319 => ${message.toString()}`);
   // let Last_Update = obj.Last_Update.toString();
   // common.log(`Tracker13319.Last_Update => ${Last_Update}`);

    switch(topic) {
      case 'InsToolAssemblyChangeHistory':
        InsToolAssemblyChangeHistory(obj.CNC_Part_Operation_Key,obj.Set_No,obj.Block_No,obj.Actual_Tool_Assembly_Life,obj.Trans_Date);      
        break;
        //  UpdateCNCPartOperationAssemblyCurrentValue
      case 'UpdateCNCPartOperationAssembly':
        UpdateCNCPartOperationAssembly(obj.CNC_Part_Operation_Key,obj.Set_No,obj.Block_No,obj.Current_Value,obj.Last_Update);      
        break;
      default:
        common.log(`Tracker13319 => topic not found!`)
        // code block
    }

  });
  
}
main();
