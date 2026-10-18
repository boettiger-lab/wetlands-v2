# Wetland Basin Explorer (config only)

A no-LLM explorer for wetlands-v2, served at `/explorer/`. Pick a HydroBASINS basin and see
where it ranks among all basins at that level on four pillars: Biodiversity, Carbon,
Resilience and Coexistence. A slider sets the basin size, from HydroBASINS level 3 (about
290 major river basins) to level 6 (about 16,200 sub-catchments). Ramsar sites are shown for
comparison: every basin is ranked, whether or not it holds a Ramsar site.

It is the wetlands counterpart of the Blue Boundaries coastal ecoregion explorer, built on the
same library and scoring.

**Status: provisional.** The variables in each pillar, their direction and the scoring are a
first cut for discussion.

This directory holds only the app's domain: `index.html`, `app.css`, `config/app.json`, one
theme per pillar plus the Ramsar overview, the SQL, and precomputed `data/`. The machinery
comes from [glen-charts](https://github.com/SchmidtDSE/glen-charts), pinned in `index.html`
(v0.2.3, the first with the slider, per-level boundary tiles and app.json overlays).
Do not write JavaScript here: features go into glen-charts.

## Units

The unit is a [HydroBASINS v1c](https://www.hydrosheds.org/products/hydrobasins) basin at
level 3, 4, 5 or 6 (the `level` param, token `{{LEVEL}}`, default 4), as H3 res-6 cells
(~36 km²):

- Level-6 basins are polyfilled by cell centre, with multipolygons split into parts first so
  islands are kept. Each res-6 cell belongs to one basin.
- Each coarser basin is the union of its level-6 basins, matched by Pfafstetter code (a
  level-n code is the first n digits of its level-6 codes) within the same HydroBASINS region
  (the first digit of `HYBAS_ID`). Code 353, Chukotka, is split at the antimeridian into two
  basins, one in region 3 and one in region 8; the list calls them "Basin 353-3" and "353-8". Polyfilling level 3 directly
  takes ~400 s on its full-resolution coastlines; the two methods agree on 99.9% of cells.
- About 1% of level-6 basins (186, all smaller than one cell, mostly small islands) get no
  cell and are not ranked. Antarctica and Greenland's ice sheet are not in HydroBASINS.
- Basins have no names. The list names each one by its Pfafstetter code and the countries
  (Overture) holding 5% or more of it, up to three: "Basin 6224 (Brazil, Peru)". The map
  hover shows the code.

Wetland, wherever it is used, is GLWD v2 classes 8–32: lacustrine, riverine, palustrine,
ephemeral, peatland and coastal wetlands. Open water (1–7) and rice paddies (33) are left out.

## Tokens

| Token | Meaning |
|---|---|
| `{{ID}}` | `HYBAS_ID` of the basin at the chosen level, the id the list returns |
| `{{LEVEL}}` | HydroBASINS level: 3, 4, 5 or 6 (`params` in `app.json`) |

## Themes and scores

Each theme has two queries, `<t>-all.sql` (every basin at `{{LEVEL}}`) and `<t>.sql` (the
same, filtered to `{{ID}}`). **Edit only `<t>-all.sql`**, then run
`bash scripts/build-explorer-sql.sh` to rewrite `<t>.sql`. The app reads precomputed
all-basins rows; a live per-basin query costs as much as the all-basins one.

Rows have `id, variable, label, value, unit, fmt, scale, dir, pct, pillar_score`, as in the
coasts explorer: `pct` is the mid-rank percentile (0–100, direction applied, ties share the
midpoint), `pillar_score` the mean of the unit's `pct`. `value` is NULL for no data and 0
for a measured absence. Values are kept to four significant figures.

| Pillar | Variable | Higher pct when |
|---|---|---|
| Biodiversity | Wetland area (GLWD v2 classes 8–32) | more wetland |
| | Wetland share of basin area | more |
| | Threatened species richness, area-weighted mean (IUCN 2025) | more species |
| | Amphibian species richness, area-weighted mean (IUCN 2025) | more species |
| Carbon | Irrecoverable carbon in wetlands (CI v2, 2024) | more carbon |
| | Irrecoverable carbon per hectare of wetland | more carbon |
| | Vulnerable carbon in wetlands (comparison; off by default) | more carbon |
| | Manageable carbon in wetlands (comparison; off by default) | more carbon |
| | Irrecoverable carbon, whole basin (comparison; off by default) | more carbon |
| Resilience | Human modification of wetlands (gHM, wetland-weighted) | less modified |
| | Projected intactness 2050 (GLOBIO MSA, SSP5-8.5) | more intact |
| | Projected warming, 2041–2070 (CHELSA, SSP3-7.0) | less warming |
| | Projected change in dry-season rainfall, 2041–2070 | less drying |
| Coexistence | Wetland area protected or conserved (WDPA + OECM) | more protected |
| | Nature's contributions to people, mean count | more |
| | Population density 2020 (GHS-POP) | fewer people |

### Carbon

Conservation International's v2 (2025) carbon has three nested pools: **manageable ⊃
vulnerable ⊃ irrecoverable** (global totals in this catalog: 777, 470 and 132 Gt C).
Manageable carbon is what human land use can affect; vulnerable is what a typical conversion
would release; irrecoverable is the part of that which could not be recovered by 2050. The
score uses irrecoverable carbon, the most decision-specific of the three: it is what a loss of
the wetland would cost for good. Manageable and vulnerable carbon are shown for comparison, at
weight 0, one click away in the leaderboard. Scoring more than one pool would count the same
carbon twice. To score manageable carbon instead, set
`leaderboard.pillars[carbon].variables.manageable_wetland_mt.weight` to 1 and the
irrecoverable ones to 0, and change the `FILTER` on `pillar_score` in `carbon-all.sql`.

Each res-8 cell's carbon is weighted by the cell's wetland share (capped at 1). The carbon
rasters are ~300 m, so a cell that is 40% wetland contributes 40% of its carbon. Licence CC
BY-NC 4.0.

Totals (wetland area, carbon) favour large basins; the share and per-hectare variables are
their size-free counterparts.

## Ramsar sites (overview card and leaderboard scope)

`config/themes/ramsar/` is the overview card on every profile: the Ramsar sites in the basin,
their area there, the share of the basin (and of its wetland) inside any site, and the count
on the Montreux Record. Ramsar does not enter any pillar score.

In the leaderboard, **All basins** ranks everything; **Basins with Ramsar sites** keeps basins
holding any part of a site and recomputes percentiles among them. The **Ramsar share** column
sorts, so high-scoring basins with no Ramsar site are easy to find. Ramsar polygons are also a
map layer, off by default (*Show Ramsar sites*).

Protected area (Coexistence) includes Ramsar sites listed in the WDPA.

## Data and licences

Every layer is public, so this app can be public. KBAs and the UNEP-WCMC seagrass and coral
layers used by the coasts explorer are left out: their licences do not allow redistribution.

## Precompute

The all-basins queries scan whole global layers (4.8 billion carbon cells per layer) and run
past the shared MCP server's 300 s limit, so they are run on a local DuckDB that answers the
same MCP `query` call. Precompute itself is the stock glen-charts CLI:

```bash
python3 scripts/local-mcp.py &     # 127.0.0.1:18765; needs ~20 GB RAM and many cores
export MCP_URL=http://127.0.0.1:18765/mcp MCP_TIMEOUT_MS=1800000
G=github:SchmidtDSE/glen-charts#v0.2.3
npx $G precompute --site explorer lists   --param level=all
npx $G precompute --site explorer results --param level=all
npx $G precompute --site explorer sources
```

Then commit `explorer/data/`, push, and restart the deployment: the pod clones `main` and
copies `explorer/` into nginx.
