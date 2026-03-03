import { describe, it, expect, beforeEach } from 'vitest';
import { QueryEngine } from '@comunica/query-sparql';
import { Store } from '@faubulous/mentor-rdf';
import { InferenceUri } from '@src/workspace/inference-uri';

// ---------------------------------------------------------------------------
// Shared graph names
// ---------------------------------------------------------------------------

const ASSERTED_GRAPH = 'workspace:/test';
const INFERENCE_GRAPH = InferenceUri.toInferenceUri(ASSERTED_GRAPH);

// ===========================================================================
// Section 1 — How filtering works (small dataset, easy to follow)
// ===========================================================================
//
// The workspace store holds two kinds of quads in distinct named graphs:
//   • asserted graph  — triples the user wrote
//   • inference graph — triples derived by the reasoner
//
// Mentor must hide the inference graph from user-facing queries while keeping
// it available internally.  Two strategies achieve this:
//
//   Approach A — filtered match() proxy
//     A thin wrapper around the mixed store that strips inference quads in
//     match().  Comunica never sees inference data; it is always invisible.
//
//   Approach B — two separate stores
//     Asserted and inferred quads live in different Store objects.  The caller
//     controls which sources Comunica receives, making inference opt-in per
//     query.
//
// ===========================================================================

// 3 asserted triples
const SMALL_ASSERTED_TURTLE = `
@prefix ex: <http://example.org/> .
ex:Dog   a ex:Animal .
ex:Cat   a ex:Animal .
ex:Eagle a ex:Animal .
`;

// 2 inferred triples (subset of asserted entities)
const SMALL_INFERRED_TURTLE = `
@prefix ex: <http://example.org/> .
ex:Dog a ex:LivingThing .
ex:Cat a ex:LivingThing .
`;

const Q_TYPES = 'SELECT DISTINCT ?type WHERE { ?s a ?type }';

describe('Section 1 — How filtering works', () => {
	let mixedStore: Store;
	let assertedStore: Store;
	let inferenceStore: Store;
	let engine: QueryEngine;

	beforeEach(async () => {
		// Approach A: one store holding both asserted and inferred quads
		mixedStore = new Store();
		await mixedStore.loadFromTurtleStream(SMALL_ASSERTED_TURTLE, ASSERTED_GRAPH);
		await mixedStore.loadFromTurtleStream(SMALL_INFERRED_TURTLE, INFERENCE_GRAPH, false);

		// Approach B: two stores, one per partition
		assertedStore = new Store();
		await assertedStore.loadFromTurtleStream(SMALL_ASSERTED_TURTLE, ASSERTED_GRAPH);

		inferenceStore = new Store();
		await inferenceStore.loadFromTurtleStream(SMALL_INFERRED_TURTLE, INFERENCE_GRAPH, false);

		engine = new QueryEngine();
	});

	// -- Approach A --

	it('A — raw mixed store exposes both Animal and LivingThing', async () => {
		const src = [{ type: 'rdfjs', value: mixedStore }];
		const result = await engine.queryBindings(Q_TYPES, { sources: src, ...OPTIONS });
		const types = (await result.toArray()).map((b: any) => b.get('type').value).sort();
		expect(types).toContain('http://example.org/Animal');
		expect(types).toContain('http://example.org/LivingThing'); // ← inference included
	});

	it('A — filtered proxy hides LivingThing (inference quads stripped in match())', async () => {
		const src = [{ type: 'rdfjs', value: makeFilteredSource(mixedStore) }];
		const result = await engine.queryBindings(Q_TYPES, { sources: src, ...OPTIONS });
		const types = (await result.toArray()).map((b: any) => b.get('type').value).sort();
		expect(types).toContain('http://example.org/Animal');
		expect(types).not.toContain('http://example.org/LivingThing'); // ← hidden
	});

	// -- Approach B --

	it('B — asserted store only: LivingThing not visible (inference store absent)', async () => {
		const src = [{ type: 'rdfjs', value: assertedStore }];
		const result = await engine.queryBindings(Q_TYPES, { sources: src, ...OPTIONS });
		const types = (await result.toArray()).map((b: any) => b.get('type').value).sort();
		expect(types).toContain('http://example.org/Animal');
		expect(types).not.toContain('http://example.org/LivingThing');
	});

	it('B — both stores: LivingThing visible (inference opt-in)', async () => {
		const src = [
			{ type: 'rdfjs', value: assertedStore },
			{ type: 'rdfjs', value: inferenceStore },
		];
		const result = await engine.queryBindings(Q_TYPES, { sources: src, ...OPTIONS });
		const types = (await result.toArray()).map((b: any) => b.get('type').value).sort();
		expect(types).toContain('http://example.org/Animal');
		expect(types).toContain('http://example.org/LivingThing'); // ← opt-in
	});
});

// ===========================================================================
// Section 2 — Performance: split stores vs single mixed store (Mentor queries)
// ===========================================================================
//
// This section benchmarks whether switching from Approach A (filtered proxy,
// single store) to Approach B (two separate stores) introduces measurable
// overhead for the query patterns Mentor actually executes.
//
// Source configurations under test:
//   • single store (raw)          — unfiltered mixedStore, baseline
//   • filtered proxy (A)          — current Mentor approach
//   • two stores, asserted only   — Approach B, default (inference excluded)
//   • two stores, both            — Approach B, inference opt-in
//
// Queries:
//   • Q_FULL_SCAN  — baseline: SELECT * WHERE { ?s ?p ?o }
//   • Q_DESCRIBE   — Mentor executeDescribeQuery (no GRAPH clause)
//
// ===========================================================================

const ASSERTED_COUNT  = 1000; // number of asserted entities
const INFERENCE_COUNT = 300;  // number of inferred entities
const RUNS = 8;

// Entity0 gets 4 extra asserted properties (5 triples total); Entity1–999 get 1 each.
const ASSERTED_TRIPLES = ASSERTED_COUNT + 4;  // 1004
// Entity0 gets 4 extra inferred properties (5 triples total); Entity1–299 get 1 each.
const INFERRED_TRIPLES = INFERENCE_COUNT + 4; // 304

// Expected Q_DESCRIBE quad counts
const DESCRIBE_ASSERTED = 5;  // asserted props on Entity0
const DESCRIBE_BOTH     = 10; // asserted + inferred props on Entity0

const ASSERTED_TURTLE = `
@prefix ex: <http://example.org/> .
ex:Entity0 a ex:Animal ; ex:name "Entity0" ; ex:size ex:Large ; ex:habitat ex:Land ; ex:diet ex:Omnivore .
${Array.from({ length: ASSERTED_COUNT - 1 }, (_, i) => `ex:Entity${i + 1} a ex:Animal .`).join('\n')}
`;

const INFERENCE_TURTLE = `
@prefix ex: <http://example.org/> .
ex:Entity0 a ex:LivingThing ; ex:kingdom ex:Animalia ; ex:domain ex:Eukaryota ; ex:class ex:Mammalia ; ex:order ex:Carnivora .
${Array.from({ length: INFERENCE_COUNT - 1 }, (_, i) => `ex:Entity${i + 1} a ex:LivingThing .`).join('\n')}
`;

const Q_FULL_SCAN = 'SELECT * WHERE { ?s ?p ?o }';
const Q_DESCRIBE  = `CONSTRUCT { <http://example.org/Entity0> ?p ?o } WHERE { <http://example.org/Entity0> ?p ?o }`;

describe('Section 2 — Performance: split stores vs single mixed store', () => {
	let mixedStore: Store;
	let assertedStore: Store;
	let inferenceStore: Store;
	let engine: QueryEngine;

	beforeEach(async () => {
		mixedStore = new Store();
		await mixedStore.loadFromTurtleStream(ASSERTED_TURTLE, ASSERTED_GRAPH);
		await mixedStore.loadFromTurtleStream(INFERENCE_TURTLE, INFERENCE_GRAPH, false);

		assertedStore = new Store();
		await assertedStore.loadFromTurtleStream(ASSERTED_TURTLE, ASSERTED_GRAPH);

		inferenceStore = new Store();
		await inferenceStore.loadFromTurtleStream(INFERENCE_TURTLE, INFERENCE_GRAPH, false);

		engine = new QueryEngine();

		// Warm up Comunica
		await countBindings(engine, [{ type: 'rdfjs', value: assertedStore }], Q_FULL_SCAN);
	});

	it('Q_FULL_SCAN — baseline: SELECT * WHERE { ?s ?p ?o }', async () => {
		const srcRaw      = [{ type: 'rdfjs', value: mixedStore }];
		const srcFiltered = [{ type: 'rdfjs', value: makeFilteredSource(mixedStore) }];
		const srcAsserted = [{ type: 'rdfjs', value: assertedStore }];
		const srcBoth     = [{ type: 'rdfjs', value: assertedStore }, { type: 'rdfjs', value: inferenceStore }];

		expect(await countBindings(engine, srcRaw,      Q_FULL_SCAN)).toBe(ASSERTED_TRIPLES + INFERRED_TRIPLES);
		expect(await countBindings(engine, srcFiltered, Q_FULL_SCAN)).toBe(ASSERTED_TRIPLES);
		expect(await countBindings(engine, srcAsserted, Q_FULL_SCAN)).toBe(ASSERTED_TRIPLES);
		expect(await countBindings(engine, srcBoth,     Q_FULL_SCAN)).toBe(ASSERTED_TRIPLES + INFERRED_TRIPLES);

		const refMs  = await avgMs(() => countBindings(engine, srcRaw,      Q_FULL_SCAN), RUNS);
		const filtMs = await avgMs(() => countBindings(engine, srcFiltered, Q_FULL_SCAN), RUNS);
		const aMs    = await avgMs(() => countBindings(engine, srcAsserted, Q_FULL_SCAN), RUNS);
		const bMs    = await avgMs(() => countBindings(engine, srcBoth,     Q_FULL_SCAN), RUNS);

		console.log(`\n--- Q_FULL_SCAN (asserted=${ASSERTED_TRIPLES}, inferred=${INFERRED_TRIPLES}, runs=${RUNS}) ---`);
		row('single store (raw)',        refMs,  refMs, `${ASSERTED_TRIPLES + INFERRED_TRIPLES} rows`);
		row('filtered proxy (A)',        filtMs, refMs, `${ASSERTED_TRIPLES} rows`);
		row('two stores, asserted (B)',  aMs,    refMs, `${ASSERTED_TRIPLES} rows`);
		row('two stores, both (B)',      bMs,    refMs, `${ASSERTED_TRIPLES + INFERRED_TRIPLES} rows`);
	}, 60_000);

	it('Q_DESCRIBE — Mentor executeDescribeQuery: no GRAPH clause', async () => {
		const srcRaw      = [{ type: 'rdfjs', value: mixedStore }];
		const srcFiltered = [{ type: 'rdfjs', value: makeFilteredSource(mixedStore) }];
		const srcAsserted = [{ type: 'rdfjs', value: assertedStore }];
		const srcBoth     = [{ type: 'rdfjs', value: assertedStore }, { type: 'rdfjs', value: inferenceStore }];

		// Entity0 has 5 asserted + 5 inferred properties
		expect(await countQuads(engine, srcRaw,      Q_DESCRIBE)).toBe(DESCRIBE_BOTH);
		expect(await countQuads(engine, srcFiltered, Q_DESCRIBE)).toBe(DESCRIBE_ASSERTED);
		expect(await countQuads(engine, srcAsserted, Q_DESCRIBE)).toBe(DESCRIBE_ASSERTED);
		expect(await countQuads(engine, srcBoth,     Q_DESCRIBE)).toBe(DESCRIBE_BOTH); // inference included

		const refMs  = await avgMs(() => countQuads(engine, srcRaw,      Q_DESCRIBE), RUNS);
		const filtMs = await avgMs(() => countQuads(engine, srcFiltered, Q_DESCRIBE), RUNS);
		const aMs    = await avgMs(() => countQuads(engine, srcAsserted, Q_DESCRIBE), RUNS);
		const bMs    = await avgMs(() => countQuads(engine, srcBoth,     Q_DESCRIBE), RUNS);

		console.log(`\n--- Q_DESCRIBE (asserted=${ASSERTED_TRIPLES}, inferred=${INFERRED_TRIPLES}, runs=${RUNS}) ---`);
		row('single store (raw)',        refMs,  refMs, `${DESCRIBE_BOTH} quads — inference included`);
		row('filtered proxy (A)',        filtMs, refMs, `${DESCRIBE_ASSERTED} quads`);
		row('two stores, asserted (B)',  aMs,    refMs, `${DESCRIBE_ASSERTED} quads`);
		row('two stores, both (B)',      bMs,    refMs, `${DESCRIBE_BOTH} quads — inference included`);
	}, 60_000);

});

// ===========================================================================
// Helpers
// ===========================================================================

const OPTIONS = { unionDefaultGraph: true };

/** Proxy that strips inference quads — mirrors SparqlConnectionService.getQuerySourceForConnection() */
function makeFilteredSource(store: Store) {
	return {
		match(s: any, p: any, o: any, g: any) {
			return (store.match(s, p, o, g) as any).filter(
				(quad: any) => !InferenceUri.isInferenceUri(quad.graph.value)
			);
		}
	};
}

async function countBindings(engine: QueryEngine, sources: any[], query: string): Promise<number> {
	return (await (await engine.queryBindings(query, { sources, ...OPTIONS })).toArray()).length;
}

async function countQuads(engine: QueryEngine, sources: any[], query: string): Promise<number> {
	return (await (await engine.queryQuads(query, { sources, ...OPTIONS })).toArray()).length;
}

async function avgMs(fn: () => Promise<unknown>, runs: number): Promise<number> {
	const t = performance.now();
	for (let i = 0; i < runs; i++) await fn();
	return (performance.now() - t) / runs;
}

function row(label: string, ms: number, ref: number, note: string) {
	const l = label.padEnd(30);
	const m = ms.toFixed(2).padStart(6);
	const r = (ms / ref).toFixed(2);
	console.log(`  ${l} ${m} ms  (${r}×)  ${note}`);
}
