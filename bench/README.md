# covenant vs sqlite comparative benchmarks

## Methodology

For each comparison point, the exact same sequence of key-value pairs are used for both covenant and sqlite.

These benchmarks use [`rusqlite`](https://docs.rs/rusqlite/latest/rusqlite/) for Rust-sqlite bindings. SQL statements'
compile times are not included in the runtime as it would be a safe assumption that any proper deployment would cache
this compilation.

For each test class, $N$ keys are generated, with the values being consecutive integers. Keys are byte sequences with
lengths within specific ranges - see `fn Size::range` in `src/data.rs` for what these specific ranges are. This is
repeated with $S$ different RNG seeds (at least for the smaller tests). Thus, the time per key is the reported total
time divided by $N * S$.

For understanding how key size affects covenant's speed, it is important to note that the keys are stored in FSTs and
that values are stored by `seqstore`. Thus, these benchmarks \*mostly* test how well FSTs get managed. Proper benchmarks
of the `seqstore` side of things will come later.

For each operation (read/write), the fastest column is bolded.

## Caveats

Benchmarking is _hard_. All of this has been run on the same machine (AMD Ryzen 9 7950X3D, WD_BLACK SN850X NVMe SSD, 128
GB DDR5), so bear in mind your performance may vary. Each case within the same run was run under the same conditions (no
other significant use on the machine), but I didn't put in overwhelming effort to reduce background usage. Each run was
done under approximately the same conditions.

Broadly speaking, these results can be used to say which one is faster and by _roughly_ how much, though saying that
(for example) "library A is 1.394x faster than library B" should be avoided - these benchmarks are here for rough
guidance, not specific results.

## Results

### $N=1000, S=50$

| Size     | Avg. key size (bytes) | covenant [write] | sqlite [write] | covenant [read] | sqlite [read] |
|----------|----------------------:|-----------------:|---------------:|----------------:|--------------:|
| Tiny     |                   3.5 |       **52.32s** |        119.68s |     **13.47ms** |      295.53ms |
| Small    |                  67.5 |       **49.77s** |        122.75s |     **22.06ms** |      272.68ms |
| Medium   |                 319.6 |       **51.32s** |        125.41s |     **86.02ms** |      281.83ms |
| Large    |                1280.1 |       **56.80s** |        133.09s |    **292.31ms** |      441.86ms |
| Huge     |                5121.9 |       **76.09s** |        125.28s |           1.06s |  **688.54ms** |
| Colossal |               43024.8 |          290.54s |    **129.38s** |           9.00s |     **3.05s** |

### $N=10000, S=5$

| Size     | Avg. key  size (bytes) | covenant [write] | sqlite [write] | covenant [read] | sqlite [read] |
|----------|-----------------------:|-----------------:|---------------:|----------------:|--------------:|
| Tiny     |                    3.5 |       **57.39s** |        114.90s |     **54.75ms** |      412.41ms |
| Small    |                   67.3 |       **53.55s** |        122.28s |     **28.65ms** |      278.63ms |
| Medium   |                  319.0 |       **55.76s** |        127.26s |     **85.82ms** |      298.87ms |
| Large    |                 1277.5 |       **64.78s** |        132.86s |    **311.51ms** |      511.77ms |
| Huge     |                 5111.4 |       **93.67s** |        124.95s |           1.19s |  **867.88ms** |
| Colossal |                42948.3 |          408.36s |    **132.42s** |           9.73s |     **4.07s** |

### $N=50000, S=1$

| Size   | Avg. key size (bytes) | covenant [write] | sqlite [write] | covenant [read] | sqlite [read] |
|--------|----------------------:|-----------------:|---------------:|----------------:|--------------:|
| Tiny   |                   3.5 |       **54.07s** |        116.33s |    **241.20ms** |      968.63ms |
| Small  |                  67.4 |       **52.27s** |        124.14s |     **30.65ms** |      311.03ms |
| Medium |                 319.2 |       **52.33s** |        121.49s |     **93.63ms** |      343.16ms |

### $N=100000, S=1$, covenant only

| Size   | Avg. key size (bytes) | covenant [write] | sqlite [write] | covenant [read] | sqlite [read] |
|--------|----------------------:|-----------------:|---------------:|----------------:|--------------:|
| Tiny   |                   3.5 |          113.50s |          _N/A_ |           1.06s |         _N/A_ |
| Small  |                  67.4 |          105.41s |          _N/A_ |         66.01ms |         _N/A_ |
| Medium |                 319.2 |          105.61s |          _N/A_ |        192.22ms |         _N/A_ |

## Remarks

Until reaching `Huge` key sizes, covenant reliably beats sqlite for speed.

For covenant, write speed seems to be fairly linear in regard to $operations$ (i.e. $N*S$), at least for keys in the
`Tiny` to `Large` categories.

For both, `Tiny` keys seem to be impacted on read speed - I don't know why it takes _longer_ than `Small` keys, as to my
mind the effect of constant overhead shouldn't cause that big an increase, but this pattern was reliably produced across
both covenant and sqlite. Discounting the `Tiny` category and focusing on the `Small` category, it _seems_ that
covenant's read speeds are slightly superlinear in regard to $N$ - some form of $N log(N)$ would make intuitive sense,
but I don't yet have enough data to empirically support that.

For the final case ($N=100,000$), sqlite was omitted (primarily due to impatience, especially considering how
consistent the prior results had been).

Overall, it seems that (for non-extreme key sizes) covenant beats sqlite in terms of speed.
