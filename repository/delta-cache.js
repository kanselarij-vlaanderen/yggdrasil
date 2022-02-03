export default class DeltaCache {
  constructor() {
    this.cache = [];
  }

  get isEmpty() {
    return this.cache.length == 0;
  }

  /**
   * Push new entries to the delta cache
   *
   * @public
  */
  push() {
    this.cache.push(...arguments);
  }

  /**
   * Clear the delta cache
   *
   * @return the current state of the cache
   * @public
  */
  clear() {
    return this.cache.splice(0, this.cache.length);
  }
}
