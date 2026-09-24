const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(__dirname + '/server.js', 'utf8');
const start = source.indexOf('async function fetchOfficialModels()');
const end = source.indexOf('\nfunction record(', start);
const config = {knownModels:['cline-pass/kept','cline-pass/removed'], selectedCatalogModels:[], removedModels:['cline-pass/removed','cline-pass/removed-custom']};
const sandbox = {config, META:{}, Date, AbortSignal, saveConfig(){}, saveMeta(){},
  activeModelIds:()=>[...config.knownModels,...config.selectedCatalogModels].filter(id=>!config.removedModels.includes(id)),
  fetchJSON:async()=>({json:{clinePass:['cline-pass/removed','cline-pass/removed-custom','cline-pass/new']}}),
  fetch:async()=>({text:async()=> 'cline-pass/removed cline-pass/removed-custom cline-pass/new'})};
vm.createContext(sandbox);
vm.runInContext(source.slice(start,end),sandbox);
(async()=>{
 const result=await sandbox.fetchOfficialModels();
 assert.deepEqual(Array.from(result.added),['cline-pass/new']);
 assert.deepEqual(Array.from(result.knownModels),['cline-pass/kept','cline-pass/new']);
 assert.equal(result.total,2);
 assert.equal(config.knownModels.includes('cline-pass/removed-custom'),false);
 console.log('PASS actual refresh merge function: removed built-in/custom models stay removed; new model added; active count correct (fixture upstreams)');
})().catch(e=>{console.error(e);process.exitCode=1});
