const startLoadTest = require('./services/incomingBulkTest.js');
const axios = require("axios");

const main = async () => {
   const response = await axios.post(
        "http://192.9.200.31:5015/api/pstn/restart-Ack",
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
  
  // while (true) {
  // for (let i = 0; i < 100; i++) {
    await startLoadTest(response);
//   }
// }
  const result = await startLoadTest(response);
  console.log(result);
};

main();