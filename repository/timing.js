const runTimed = async function(callback) {
  const start = new Date();
  await callback();
  const end = new Date();
  return (end - start) / 1000; // in seconds
};

const runStage = async function(message, callback, graph = null) {
  const duration = await runTimed(callback);
  if (graph)
    console.log(`${graph} => ${message} -- time: ${duration.toFixed(3)}s`);
  else
    console.log(`${message} -- time: ${duration.toFixed(3)}s`);
};

export {
  runTimed,
  runStage
}
