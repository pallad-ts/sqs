const TestRunner = require('jest-runner');

class SerialTestRunner extends TestRunner {
    runTests(tests, watcher, onStart, onResult, onFailure, options) {
        return super.runTests(tests, watcher, onStart, onResult, onFailure, {
            ...options,
            serial: true
        });
    }

    get isSerial() {
        return true;
    }
}

module.exports = SerialTestRunner;