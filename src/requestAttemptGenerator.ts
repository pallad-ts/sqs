import {ulid} from "ulid";

export function ulidPrefixedFactory() {
    const prefix = ulid();
    let attempt = 0;
    return function () {
        return prefix + attempt++;
    }
}