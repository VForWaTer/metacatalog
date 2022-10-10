CREATE OR REPLACE VIEW locations AS
SELECT entries.id,
	entries.location,
	spatial_scales.extent,
	ST_Envelope(spatial_scales.extent) "bbox",
	ST_Centroid(spatial_scales.extent) "centroid",
	ST_Area(spatial_scales.extent) * 0.3048 ^ 2 "area_sqm"
FROM entries
INNER JOIN datasources ON entries.datasource_id = datasources.id
INNER JOIN spatial_scales ON spatial_scales.id = datasources.spatial_scale_id