import {create} from "@pallad/id";

export function ulidPrefixedFactory() {
	const prefix = create();
	let attempt = 0;
	return function () {
		return `${prefix}${attempt++}`;
	}
}
