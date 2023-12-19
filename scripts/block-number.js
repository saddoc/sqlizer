function gen(min, max) {
  var num = Math.floor(Math.random() * (max - min + 1)) + min;
  var data = {
    "jsonrpc":"2.0","method":"eth_getBlockByNumber",
    "params":["0x"+num.toString(16),true],"id":1};
  return JSON.stringify(data);
}
gen(36640000, 36680000);

