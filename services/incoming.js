const axios = require("axios");
const kafkaMessaging = require("../connections/kafka");
const redisConnection = require ("../connections/redis");
const startLoadTest = require("./incomingBulkTest");
const { log } = require("node:console");
const redis=redisConnection.getClient();

async function startLoadTest(){
try{
    const key = "omni:test:seq"
    const exist= await redis.exists(key);
    if (!exist){
        await redis.set(key,0);
    }
    const seq_num= Number(await redis.get(key));

    const response = await axios.post("http://192.9.200.31:5015/api/pstn/restart-Ack",
        {
        header: {
          mtyp: 20,
          mfrm: 0,
          ip: "192.9.200.234",
          vers: "1.0.0.0",
          strt: "2026-03-30T10:10:30.001Z",
          actt: 0,
          tchs: 95,
        },
      }
    );
    console.log("api calling success");

    const token = response.data.header.tokn;
    console.log("token:",token);

    const topics=Object.values(response.data.body.bsnq);
    console.log("topics:",topics);


    for (const topic of topics){
        console.log("topic to :",topic);
        const publicPromise=[];
        
        
    }
    
    
    



}catch(err){

}
}