const startLoadTest = require('./services/incomingBulkTest.js');

async function main() {

  const result = await startLoadTest();

  console.log(result);

}

main();