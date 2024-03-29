CREATE OR REPLACE VIEW locations AS
select 
	t.*,
	st_area(t.geom::geography) as "area_sqm",
	st_asewkt(t.point_location) AS point_location_st_asewkt
from 
(SELECT 
entries.id,
case when entries.location is not null then
	entries.location
else
	case when spatial_scales.extent is not null then
		st_centroid(spatial_scales.extent)
	else 
		null::geometry
	end
end  
as point_location,
case when spatial_scales.extent is not null then
	spatial_scales.extent
else 
	entries.location
end 
as "geom"
FROM entries
LEFT JOIN datasources ON entries.datasource_id = datasources.id
LEFT JOIN spatial_scales ON spatial_scales.id = datasources.spatial_scale_id
 ) as t

order by t.id asc