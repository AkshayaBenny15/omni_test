const startLoadTest = require('./services/incomingBulkTest.js');

const main = async () => {
  const result = await startLoadTest();
  console.log(result);
};

main();