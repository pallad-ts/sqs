module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',

    testMatch: [
        "**/*Test.ts"
    ],
    moduleFileExtensions: [
        'js', 'ts'
    ],
    runner: './serialRunner.js'
};