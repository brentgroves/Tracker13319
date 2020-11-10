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
const MYSQL_DATABASE= "Plex";
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
set @CNC_Approved_Workcenter_Key = 1;
set @Set_No = 1;
set @Block_No = 1;
set @Actual_Tool_Life = 2;
set @Run_Date = '2020-08-18 00:00:01';
-- CNC_Approved_Workcenter_Key=1,Set_No=1,Block_No=1,Current_Value=18136,Last_Update=2020-08-25 10:38:27
-- "CNC_Approved_Workcenter_Key":1,"Set_No":1,"Block_No":7,"Current_Value":29392,"Run_Date":"2020-08-25 10:17:55"
-- select a.* from CNC_Part_Operation_Assembly a
CALL InsToolLifeHistory(@CNC_Approved_Workcenter_Key,@Set_No,@Block_No,@Actual_Tool_Life,@Run_Date,@Tool_Life_Key,@Return_Value);
	 -- UpdateCNCToolOpPartLifeCurrentValue(?,?,?,?,?,@ReturnValue); select @ReturnValue as pReturnValue
SELECT @Tool_Life_Key,@Return_Value;

*/

async function InsToolLifeHistory(CNC_Approved_Workcenter_Key,Set_No,Block_No,Run_Quantity,Run_Date) {
  let conn;
  try {
    conn = await pool.getConnection();      
    console.log(`In InsToolLifeHistory with params CNC_Approved_Workcenter_Key=${CNC_Approved_Workcenter_Key},Set_No=${Set_No},Block_No=${Block_No},Run_Quantity=${Run_Quantity},Run_Date=${Run_Date}`)
    // const someRows = await conn.query('call UpdateCNCToolOpPartLifeCurrentValue(1,1,1,6,"2020-08-25 10:17:55",@ReturnValue); select @Tool_Life_Key as pTool_Life_Key, @ReturnValue as pReturnValue');
    const someRows = await conn.query('call InsToolLifeHistory(?,?,?,?,?,@Tool_Life_Key,@Return_Value); select @Tool_Life_Key as pTool_Life_Key,@Return_Value as pReturn_Value',[CNC_Approved_Workcenter_Key,Set_No,Block_No,Run_Quantity,Run_Date]);
    let returnValue = someRows[1][0].pReturn_Value;
    let toolLifeKey = someRows[1][0].pTool_Life_Key;
    console.log(`InsToolLifeHistory.returnValue=${returnValue}`);
    console.log(`InsToolLifeHistory.ToolLifeKey=${toolLifeKey}`);
  } catch (err) {
    // handle the error
    console.log(`Error =>${err}`);
  } finally {
    if (conn) conn.release(); //release to pool
  }
}
async function InsToolLifeHistoryV2(CNC_Approved_Workcenter_Key,Tool_Var,Run_Quantity,Run_Date) {
  let conn;
  try {
    conn = await pool.getConnection();      
    console.log(`In InsToolLifeHistoryV2 with params CNC_Approved_Workcenter_Key=${CNC_Approved_Workcenter_Key},Tool_Var=${Tool_Var},Run_Quantity=${Run_Quantity},Run_Date=${Run_Date}`)
    const someRows = await conn.query('call InsToolLifeHistoryV2(?,?,?,?,@Tool_Life_Key,@Return_Value); select @Tool_Life_Key as pTool_Life_Key,@Return_Value as pReturn_Value',[CNC_Approved_Workcenter_Key,Tool_Var,Run_Quantity,Run_Date]);
    let returnValue = someRows[1][0].pReturn_Value;
    let toolLifeKey = someRows[1][0].pTool_Life_Key;
    console.log(`InsToolLifeHistoryV2.returnValue=${returnValue}`);
    console.log(`InsToolLifeHistoryV2.ToolLifeKey=${toolLifeKey}`);
  } catch (err) {
    // handle the error
    console.log(`Error =>${err}`);
  } finally {
    if (conn) conn.release(); //release to pool
  }
}
async function UpdateCNCToolOpPartLife(CNC_Approved_Workcenter_Key,Set_No,Block_No,Current_Value,Running_Total,Last_Update) {
  let conn;
  try {
    conn = await pool.getConnection();      
    console.log(`In UpdateCNCToolOpPartLife with params CNC_Approved_Workcenter_Key=${CNC_Approved_Workcenter_Key},Set_No=${Set_No},Block_No=${Block_No},Current_Value=${Current_Value},Running_Total=${Running_Total},Last_Update=${Last_Update}`)
    // const someRows = await conn.query('call UpdateCNCToolOpPartLifeCurrentValue(1,1,1,6,"2020-08-25 10:17:55",@ReturnValue); select @ReturnValue as pReturnValue');
    const someRows = await conn.query('call UpdateCNCToolOpPartLife(?,?,?,?,?,?,@ReturnValue); select @ReturnValue as pReturnValue',[CNC_Approved_Workcenter_Key,Set_No,Block_No,Current_Value,Running_Total,Last_Update]);
    let returnValue = someRows[1][0].pReturnValue
    console.log(`UpdateCNCToolOpPartLife.returnValue=${returnValue}`);
  } catch (err) {
    // handle the error
    console.log(`Error =>${err}`);
  } finally {
    if (conn) conn.release(); //release to pool
  }
}
/*
CREATE PROCEDURE InsAssemblyMachiningHistory
(
	IN pCNC_Approved_Workcenter_Key INT,  
	IN pPallet_No INT,
	IN pTool_Var INT,
	IN pStart_Time datetime,
	IN pEnd_Time datetime,
	OUT pAssembly_Machining_History_Key INT,
	OUT pReturnValue INT 
)
*/
async function InsAssemblyMachiningHistory(CNC_Approved_Workcenter_Key,Pallet_No,Tool_Var,Start_Time,End_Time) {
  let conn;
  try {
    conn = await pool.getConnection();      
    console.log(`In InsAssemblyMachiningHistory with params CNC_Approved_Workcenter_Key=${CNC_Approved_Workcenter_Key},Pallet_No=${Pallet_No},Tool_Var=${Tool_Var},Start_Time=${Start_Time},End_Time=${End_Time}`);    
    const someRows = await conn.query('call InsAssemblyMachiningHistory(?,?,?,?,?,@Assembly_Machining_History_Key,@Return_Value); select @Assembly_Machining_History_Key as pAssembly_Machining_History_Key,@Return_Value as pReturn_Value',[CNC_Approved_Workcenter_Key,Pallet_No,Tool_Var,Start_Time,End_Time]);
    let returnValue = someRows[1][0].pReturn_Value;
    let Assembly_Machining_History_Key = someRows[1][0].pAssembly_Machining_History_Key;
    console.log(`InsAssemblyMachiningHistory.returnValue=${returnValue}`);
    console.log(`InsAssemblyMachiningHistory.Assembly_Machining_History_Key=${Assembly_Machining_History_Key}`);
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
    mqttClient.subscribe('InsToolLifeHistory', function(err) {
      if (!err) {
        common.log('Tracker13319 has subscribed to: InsToolLifeHistory');
      }
    });                 //UpdateCNCToolOpPartLife
    mqttClient.subscribe('InsToolLifeHistoryV2', function(err) {
      if (!err) {
        common.log('Tracker13319 has subscribed to: InsToolLifeHistory');
      }
    });                 //UpdateCNCToolOpPartLife
    mqttClient.subscribe('UpdateCNCToolOpPartLife', function(err) {
      if (!err) {
        common.log('Tracker13319 has subscribed to: UpdateCNCToolOpPartLife');
      }
    });
    mqttClient.subscribe('InsAssemblyMachiningHistory', function(err) {
      if (!err) {
        common.log('Tracker13319 has subscribed to: InsAssemblyMachiningHistory');
      }
    });
  });
  /*
      let tcMsg = {
      CNC_Approved_Workcenter_Key: nCNCApprovedWorkcenterKey,
      Pallet_No: nPalletNo,
      Tool_Var: nToolVar,
      Start_Time:
        Assembly_Machining_History[nCNCApprovedWorkcenterKey][nPalletNo][
          nToolVar
        ].Start_Time,
      End_Time: transDate,
    };

    let tcMsgString = JSON.stringify(tcMsg);
    common.log(`Published InsAssemblyMachiningHistory => ${tcMsgString}`);
CREATE PROCEDURE InsAssemblyMachiningHistory
(
	IN pCNC_Approved_Workcenter_Key INT,  
	IN pPallet_No INT,
	IN pTool_Var INT,
	IN pStart_Time datetime,
	IN pEnd_Time datetime,
	OUT pAssembly_Machining_History_Key INT,
	OUT pReturnValue INT 
)

  */

  // message is a buffer
  mqttClient.on('message', function(topic, message) {
    const obj = JSON.parse(message.toString()); // payload is a buffer
    common.log(`Tracker13319 => ${message.toString()}`);
   // let Last_Update = obj.Last_Update.toString();
   // common.log(`Tracker13319.Last_Update => ${Last_Update}`);

    switch(topic) {
      case 'InsToolLifeHistory':
        InsToolLifeHistory(obj.CNC_Approved_Workcenter_Key,obj.Set_No,obj.Block_No,obj.Run_Quantity,obj.Run_Date);      
        break;
        case 'InsToolLifeHistoryV2':
          InsToolLifeHistoryV2(obj.CNC_Approved_Workcenter_Key,
            obj.Tool_Var,obj.obj.Run_Quantity,obj.Run_Date);      
          break;
            //UpdateCNCToolOpPartLife
      case 'UpdateCNCToolOpPartLife':
        UpdateCNCToolOpPartLife(obj.CNC_Approved_Workcenter_Key,obj.Set_No,obj.Block_No,obj.Current_Value,obj.Running_Total,obj.Last_Update);      
        break;
        //InsAssemblyMachiningHistory
        case 'InsAssemblyMachiningHistory':
          InsAssemblyMachiningHistory(obj.CNC_Approved_Workcenter_Key,obj.Pallet_No,obj.Tool_Var,obj.Start_Time,obj.End_Time);      
          break;
        default:
        common.log(`Tracker13319 => topic not found!`)
        // code block
    }

  });
  
}
main();
