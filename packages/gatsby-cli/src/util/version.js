const { setDefaultTags } = require(`gatsby-telemetry`)
const path = require(`path`)

let localGatsbyVersionCache
exports.getLocalGatsbyVersion = () => {
  if (localGatsbyVersionCache) {
    return localGatsbyVersionCache
  }

  try {
    const packageInfo = require(path.join(
      process.cwd(),
      `node_modules`,
      `gatsby`,
      `package.json`
    ))
    localGatsbyVersionCache = packageInfo.version

    try {
      setDefaultTags({ installedGatsbyVersion: localGatsbyVersionCache })
    } catch (e) {
      // ignore
    }
  } catch (err) {
    /* ignore */
  }

  return localGatsbyVersionCache
}

let localSharpVersionCache
exports.getLocalSharpVersion = () => {
  if (localSharpVersionCache) {
    return localSharpVersionCache
  }

  try {
    // Sharp may not be installed. That's ok.
    const packageInfo = require(path.join(
      process.cwd(),
      `node_modules`,
      `sharp`,
      `package.json`
    ))
    localSharpVersionCache = packageInfo.version
  } catch (err) {
    /* ignore */
  }

  return localSharpVersionCache
}

let currentGatsbyCliVersionCache
exports.getCurrentCliVersion = () => {
  if (currentGatsbyCliVersionCache) {
    return currentGatsbyCliVersionCache
  }

  const { version } = require(`../../package.json`)
  currentGatsbyCliVersionCache = version
  return currentGatsbyCliVersionCache
}

exports.getCurrentNodeVersion = () => process.version
