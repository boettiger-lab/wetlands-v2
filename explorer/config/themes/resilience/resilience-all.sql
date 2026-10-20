-- Resilience pillar, every HydroBASINS basin at one level.
-- One row per (id, variable). pct = mid-rank percentile (0-100) among all basins with data,
-- direction applied so higher = more resilient (dir = -1 ranks low modification and low
-- warming high). pillar_score = mean pct (PROVISIONAL).
-- Unit = a HydroBASINS v1c basin at level {{LEVEL}} (3-6), as H3 res-6 cells (~36 km²).
-- Level-6 basins are polyfilled by cell centre (multipolygons split into parts first, so
-- islands count); each coarser basin is the union of its level-6 basins, matched by
-- Pfafstetter code (a level-n code is the first n digits of its level-6 codes) within the same
-- HydroBASINS region (the first digit of HYBAS_ID: code 353, Chukotka, is split at the
-- antimeridian into regions 3 and 8). Each res-6 cell belongs to one basin. About 1% of
-- level-6 basins, all smaller than one cell, get none.
WITH cells6 AS MATERIALIZED (
  SELECT DISTINCT PFAF_ID AS pfaf6, HYBAS_ID // 1000000000 AS region,
         UNNEST(h3_polygon_wkt_to_cells_experimental(ST_AsText(geom), 6, 'center'))::UBIGINT AS h6
  FROM (SELECT PFAF_ID, HYBAS_ID, UNNEST(ST_Dump(geometry), recursive := true)
        FROM read_parquet('s3://public-hydrobasins/level_06.parquet'))
),
basins AS MATERIALIZED (
  SELECT l.HYBAS_ID AS id, c.h6, h3_cell_to_parent(c.h6, 0)::BIGINT AS h0, h3_cell_area(c.h6, 'km^2') AS a
  FROM cells6 c
  JOIN read_parquet('s3://public-hydrobasins/level_0{{LEVEL}}.parquet') l
    ON l.PFAF_ID = c.pfaf6 // CAST(POW(10, 6 - {{LEVEL}}) AS INTEGER)
   AND l.HYBAS_ID // 1000000000 = c.region
),
units AS (SELECT id, SUM(a) AS area_km2 FROM basins GROUP BY id),
-- Natural wetland area per res-8 cell: GLWD v2 classes 8-32 (lacustrine, riverine, palustrine,
-- ephemeral, peatland and coastal wetlands). Open water (1-7) and rice paddies (33) are left out.
wet8 AS MATERIALIZED (
  SELECT w.h0, w.h8, ANY_VALUE(w.h6) AS h6, SUM(w.area_ha_x10) / 1000.0 AS km2
  FROM read_parquet('s3://public-wetlands/glwd/class-area-hex/Z=*/h0=*/data_0.parquet') w
  WHERE w.Z BETWEEN 8 AND 32
  GROUP BY w.h0, w.h8
),
-- Global human modification (gHM ~2016, 0-1) over the basin's wetlands: the mean over
-- res-8 cells, weighted by each cell's wetland area. No value without mapped wetland.
ghm AS (
  SELECT b.id, SUM(g.ghm * w.km2) / SUM(w.km2) AS v
  FROM read_parquet('s3://public-human-modification/ghm/hex/h0=*/data_0.parquet') g
  JOIN wet8 w ON g.h0 = w.h0 AND g.h8 = w.h8
  JOIN basins b ON b.h0 = w.h0 AND b.h6 = w.h6
  WHERE NOT isnan(g.ghm) AND w.km2 > 0
  GROUP BY b.id
),
-- Projected biodiversity intactness 2050 (GLOBIO 4 MSA, SSP5-RCP8.5, 0-1), area-weighted
-- mean over the basin.
msa AS (
  SELECT b.id, SUM(m.msa * h3_cell_area(m.h8, 'km^2')) / SUM(h3_cell_area(m.h8, 'km^2')) AS v
  FROM read_parquet('s3://public-globio/globio-msa-ssp5rcp85-2050-overall/hex/h0=*/data_0.parquet') m
  JOIN basins b ON b.h0 = m.h0 AND b.h6 = h3_cell_to_parent(m.h8, 6)
  WHERE NOT isnan(m.msa)
  GROUP BY b.id
),
-- Climate exposure, CHELSA 2.1: 2041-2070 under SSP3-7.0 (median of five models) against
-- the 1981-2010 baseline. Annual mean temperature (bio1) and precipitation of the driest
-- quarter (bio17), area-weighted per res-6 cell first.
clim_base AS (
  SELECT h0, h3_cell_to_parent(h8, 6) AS h6, SUM(bio1 * h3_cell_area(h8, 'km^2')) AS t,
         SUM(bio17 * h3_cell_area(h8, 'km^2')) AS p, SUM(h3_cell_area(h8, 'km^2')) AS a
  FROM read_parquet('s3://public-bioclimate/chelsa-2-1/baseline-1981-2010/hex/h0=*/data_0.parquet')
  WHERE NOT isnan(bio1) AND NOT isnan(bio17)
  GROUP BY ALL
),
clim_fut AS (
  SELECT h0, h3_cell_to_parent(h8, 6) AS h6, SUM(bio1_median * h3_cell_area(h8, 'km^2')) AS t,
         SUM(bio17_median * h3_cell_area(h8, 'km^2')) AS p, SUM(h3_cell_area(h8, 'km^2')) AS a
  FROM read_parquet('s3://public-bioclimate/chelsa-2-1/ssp370-2041-2070/hex/h0=*/data_0.parquet')
  WHERE NOT isnan(bio1_median) AND NOT isnan(bio17_median)
  GROUP BY ALL
),
clim AS (
  SELECT b.id, SUM(f.t) / SUM(f.a) - SUM(c.t) / SUM(c.a) AS warming,
         CASE WHEN SUM(c.p) > 0 THEN SUM(f.p) / SUM(c.p) - 1 END AS dry_change
  FROM basins b
  JOIN clim_base c ON c.h0 = b.h0 AND c.h6 = b.h6
  JOIN clim_fut f ON f.h0 = b.h0 AND f.h6 = b.h6
  GROUP BY b.id
),
vals AS (
  SELECT u.id, 'human_modification_wetlands' AS variable, 'Human modification of wetlands' AS label,
         ghm.v AS value, 'index 0–1' AS unit, 'num2' AS fmt, 'linear' AS scale, -1 AS dir
  FROM units u LEFT JOIN ghm USING (id)
  UNION ALL
  SELECT u.id, 'msa_2050', 'Projected intactness in 2050',
         msa.v, 'index 0–1', 'num2', 'linear', 1
  FROM units u LEFT JOIN msa USING (id)
  UNION ALL
  SELECT u.id, 'warming_2055', 'Projected warming by 2041–2070',
         clim.warming, '°C', 'num1', 'linear', -1
  FROM units u LEFT JOIN clim USING (id)
  UNION ALL
  SELECT u.id, 'dry_season_precip_change', 'Projected change in dry-season rainfall by 2041–2070',
         clim.dry_change, 'share', 'pct', 'linear', 1
  FROM units u LEFT JOIN clim USING (id)
),
-- Score: mid-rank percentile, 0-100, among units with data. Tied values share
-- the midpoint of their rank range (plain PERCENT_RANK would put every tie at
-- the bottom of the range, e.g. every basin with no protected wetland).
ranked AS (
  SELECT *, CASE WHEN value IS NULL THEN NULL
                 ELSE ROUND(100 * ((RANK() OVER w - 1) + (COUNT(*) OVER w - 1)) / 2.0
                            / NULLIF(COUNT(*) OVER (PARTITION BY variable, value IS NULL) - 1, 0), 1)
            END AS pct
  FROM vals
  WINDOW w AS (PARTITION BY variable, value IS NULL ORDER BY dir * value)
)
-- value is kept to 4 significant figures, which keeps the level-6 files small
SELECT id, variable, label,
       CASE WHEN value IS NULL OR value = 0 THEN value
            ELSE ROUND(value, 3 - CAST(FLOOR(LOG10(ABS(value))) AS INTEGER)) END AS value,
       unit, fmt, scale, dir, pct,
       ROUND(AVG(pct) OVER (PARTITION BY id), 1) AS pillar_score
FROM ranked
ORDER BY id, variable
