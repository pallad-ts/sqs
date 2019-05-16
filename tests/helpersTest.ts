import {assert} from 'chai';
import * as faker from 'faker';
import {createDeduplicationIdForRetry} from "../src/helpers";

describe('helpers', () => {
    it('createDeduplicationIdForRetry', () => {
        const dedupId = faker.random.alphaNumeric(40);

        assert.strictEqual(createDeduplicationIdForRetry(dedupId, 1), dedupId + '_alphaRetry1');
        assert.strictEqual(createDeduplicationIdForRetry(dedupId + '_alphaRetry1', 2), dedupId + '_alphaRetry2');
        assert.strictEqual(createDeduplicationIdForRetry(dedupId + '_alphaRetry100', 101), dedupId + '_alphaRetry101');
    })
});