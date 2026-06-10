const axios = require("axios");
const startLoadTest = require("./services/incomingBulkTest");

const main = async () => {
    try {

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
                    tchs: 95
                }
            }
        );

 while (true) {
    try {
        const result = await startLoadTest(response);
        console.log(result);
    } catch (err) {
        console.error("Load test failed:", err);
    }

    // await new Promise(resolve => setTimeout(resolve, 1000));
}
    } catch (err) {

        console.error(err);
    }
};

main();