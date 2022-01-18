import cliProgress from 'cli-progress';

const runTimed = async function(callback) {
  const start = new Date();
  await callback();
  const end = new Date();
  return (end - start) / 1000; // in seconds
};

const runStage = async function(message, callback, actor = null) {
  const prefix = actor ? `${actor} => ` : '';
  console.log(`${prefix}${message} -- started`);
  const duration = await runTimed(callback);
  console.log(`${prefix}${message} -- finished in ${duration.toFixed(3)}s`);
};

const forLoopProgressBar = async function(array, callback) {
  const bar = new cliProgress.SingleBar({ noTTYOutput: true, notTTYSchedule: 8000 }, cliProgress.Presets.rect);
  bar.start(array.length, 0);
  try {
    for (let i = 0; i < array.length; i++) {
      const item = array[i];
      await callback(item);
      bar.increment();
    }
  } catch (e) {
    throw e;
  } finally {
    bar.stop();
  }
};

export {
  runTimed,
  runStage,
  forLoopProgressBar
}
